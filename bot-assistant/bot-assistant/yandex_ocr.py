# -*- coding: utf-8 -*-
"""
Yandex Vision OCR — читает изображение накладной и возвращает распознанный текст.

Используется как первый шаг в recognize_invoice:
    text = await yandex_read_text(image_bytes)
    # затем отдаём text в текстовый LLM для извлечения структуры

API endpoint: https://ocr.api.cloud.yandex.net/ocr/v1/recognizeText
Документация: https://aistudio.yandex.ru/docs/ru/vision/operations/ocr/text-detection-table

Ключевой факт: Yandex в режиме model="table" возвращает в ответе поле
`textAnnotation.tables[]` — уже структурированную таблицу с rowIndex/columnIndex
по ячейкам. Это на порядок надёжнее ручной кластеризации по Y-координате и
даёт LLM готовую разметку рядов и колонок.
"""
import os
import io
import base64
import logging
import httpx

logger = logging.getLogger(__name__)

OCR_URL = "https://ocr.api.cloud.yandex.net/ocr/v1/recognizeText"


async def _ocr_request(img_bytes: bytes, model: str) -> dict:
    """Один вызов Yandex OCR. Возвращает распарсенный JSON."""
    key = os.getenv("YANDEX_KEY", "")
    folder = os.getenv("YANDEX_FOLDER", "")
    if not key or not folder:
        raise RuntimeError("YANDEX_KEY / YANDEX_FOLDER не заданы в окружении")

    img_b64 = base64.b64encode(img_bytes).decode()
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            OCR_URL,
            headers={
                "Authorization": f"Api-Key {key}",
                "x-folder-id": folder,
                "x-data-logging-enabled": "true",
                "Content-Type": "application/json",
            },
            json={
                "mimeType": "JPEG",
                "languageCodes": ["ru", "en"],
                "model": model,
                "content": img_b64,
            },
        )
        return resp.json()


def _extract_lines(response: dict) -> list:
    """
    Парсит ответ Yandex OCR в список строк Yandex (line.text) с bbox.
    Используется для (а) детекции поворота и (б) fallback-вывода, если
    в ответе нет структурированных таблиц.
    """
    try:
        blocks = response["result"]["textAnnotation"]["blocks"]
    except (KeyError, TypeError):
        return []
    lines_out = []
    for block in blocks:
        for line in block.get("lines", []):
            text = (line.get("text") or "").strip()
            if not text:
                continue
            verts = line.get("boundingBox", {}).get("vertices", [])
            if len(verts) < 4:
                continue
            ys = [int(v.get("y", 0)) for v in verts]
            xs = [int(v.get("x", 0)) for v in verts]
            y_min = min(ys)
            x_min = min(xs)
            h = max(1, max(ys) - y_min)
            w = max(1, max(xs) - x_min)
            lines_out.append({"text": text, "y": y_min, "x": x_min, "w": w, "h": h})
    return lines_out


def _median_aspect_ratio(lines: list) -> float:
    """
    Медианное соотношение w/h строк Yandex. >1 = горизонтальный текст,
    <1 = вертикальный (картинка повёрнута). Учитываем только строки с
    реальным содержимым (>=2 символов), чтобы одиночные цифры не искажали.
    """
    ratios = sorted(l["w"] / l["h"] for l in lines if len(l["text"]) >= 2)
    if not ratios:
        return 1.0
    return ratios[len(ratios) // 2]


def _cell_bbox(cell: dict) -> dict:
    """Извлекает координаты bbox из ячейки Yandex (x_min, x_max, y_min, y_max, centers)."""
    verts = cell.get("boundingBox", {}).get("vertices", [])
    if len(verts) < 4:
        return None
    xs = [int(v.get("x", 0)) for v in verts]
    ys = [int(v.get("y", 0)) for v in verts]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    return {
        "x_min": x_min, "x_max": x_max,
        "y_min": y_min, "y_max": y_max,
        "x_center": (x_min + x_max) / 2,
        "y_center": (y_min + y_max) / 2,
    }


def _build_table_infos(response: dict) -> list:
    """
    Парсит все таблицы из ответа Yandex и возвращает список table_info.
    Каждый table_info = {matrix, bboxes, rows, cols}.
    matrix[r][c]  — текст ячейки.
    bboxes[r][c]  — координаты ячейки (None если ячейка пустая).
    """
    try:
        tables = response["result"]["textAnnotation"].get("tables", []) or []
    except (KeyError, TypeError):
        return []
    result = []
    for tbl in tables:
        row_count = int(tbl.get("rowCount", 0) or 0)
        col_count = int(tbl.get("columnCount", 0) or 0)
        cells = tbl.get("cells", []) or []
        if row_count <= 0 or col_count <= 0:
            continue
        matrix = [["" for _ in range(col_count)] for _ in range(row_count)]
        bboxes = [[None for _ in range(col_count)] for _ in range(row_count)]
        for c in cells:
            try:
                r = int(c.get("rowIndex", 0))
                co = int(c.get("columnIndex", 0))
            except (TypeError, ValueError):
                continue
            if not (0 <= r < row_count and 0 <= co < col_count):
                continue
            text = (c.get("text") or "").replace("\n", " ").strip()
            if matrix[r][co]:
                matrix[r][co] += " " + text
            else:
                matrix[r][co] = text
            bbox = _cell_bbox(c)
            if bbox and not bboxes[r][co]:
                bboxes[r][co] = bbox
        result.append({
            "matrix": matrix, "bboxes": bboxes,
            "rows": row_count, "cols": col_count,
        })
    return result


def _find_empty_order_table(table_infos: list):
    """
    Ищет таблицу с пустой колонкой "Заказ". Возвращает (table_info, col_idx)
    или (None, None) если не найдено.
    """
    import re
    for ti in table_infos:
        matrix = ti["matrix"]
        if ti["rows"] <= 1:
            continue
        for col_idx in range(ti["cols"]):
            if re.search(r"заказ", matrix[0][col_idx] or "", re.I):
                body = [matrix[r][col_idx].strip() for r in range(1, ti["rows"])]
                empties = sum(1 for v in body if not v)
                if body and empties / len(body) > 0.5:
                    return ti, col_idx
    return None, None


def _fill_handwritten_orders(table_info: dict, order_col: int, hw_response: dict) -> int:
    """
    Заполняет пустые ячейки колонки "Заказ" в table_info рукописными
    числами, распознанными моделью handwritten. Использует координаты:
    - X-диапазон колонки Заказ берётся из bboxes ячеек этой колонки.
    - Y-центры строк товаров — из ячеек столбца "Наименование".
    - Рукописные line из hw_response отбираются по регексу целого 1-999
      И X-центр должен попадать в X-диапазон колонки Заказ.
    - Для каждого отобранного числа ищется ближайшая по Y строка товара.

    Возвращает число заполненных ячеек.
    """
    import re
    matrix = table_info["matrix"]
    bboxes = table_info["bboxes"]
    rows = table_info["rows"]
    cols = table_info["cols"]

    # X-диапазон колонки Заказ по всем непустым bboxes этой колонки.
    order_xs_min, order_xs_max = [], []
    for r in range(rows):
        bb = bboxes[r][order_col]
        if bb:
            order_xs_min.append(bb["x_min"])
            order_xs_max.append(bb["x_max"])
    if not order_xs_min:
        return 0
    x_lo, x_hi = min(order_xs_min), max(order_xs_max)
    pad = max(5, int((x_hi - x_lo) * 0.3))
    x_lo -= pad
    x_hi += pad

    # Колонка "Наименование" — обычно первая после №, ищем по заголовку.
    name_col = None
    for col_idx in range(cols):
        if re.search(r"наименование|товар", matrix[0][col_idx] or "", re.I):
            name_col = col_idx
            break
    if name_col is None:
        name_col = 0

    # Y-центры строк данных (берём bbox наименования, fallback — любая ячейка строки).
    row_y_centers = []  # [(row_idx, y_center)]
    heights = []
    for r in range(1, rows):
        bb = bboxes[r][name_col]
        if not bb:
            for co in range(cols):
                if bboxes[r][co]:
                    bb = bboxes[r][co]
                    break
        if bb:
            row_y_centers.append((r, bb["y_center"]))
            heights.append(bb["y_max"] - bb["y_min"])
    if not row_y_centers or not heights:
        return 0
    median_h = sorted(heights)[len(heights) // 2]

    # Получаем lines handwritten-ответа с координатами.
    hw_lines = _extract_lines(hw_response)

    filled = 0
    for line in hw_lines:
        txt = line["text"].strip().rstrip(".")
        # Только целые числа от 1 до 999 — рукописное количество упаковок.
        if not re.fullmatch(r"\d{1,3}", txt):
            continue
        qty = int(txt)
        if qty == 0:
            continue
        x_center = line["x"] + line["w"] / 2
        if not (x_lo <= x_center <= x_hi):
            continue
        y_center = line["y"] + line["h"] / 2
        # Ближайшая по Y строка товара.
        closest_row, dist = None, float("inf")
        for r, yc in row_y_centers:
            d = abs(yc - y_center)
            if d < dist:
                dist = d
                closest_row = r
        if closest_row is None or dist > median_h * 1.5:
            continue
        # Не перетираем уже заполненные ячейки и не дублируем.
        if not matrix[closest_row][order_col]:
            matrix[closest_row][order_col] = str(qty)
            filled += 1
    return filled


def _render_table_infos(table_infos: list) -> str:
    """Рендерит все table_infos в pipe-separated текст, по одной таблице."""
    parts = []
    for ti in table_infos:
        parts.append("\n".join(" | ".join(row) for row in ti["matrix"]))
    return "\n\n".join(parts)


def _render_tables(response: dict) -> str:
    """
    Превращает textAnnotation.tables[] в pipe-separated таблицы (по одной
    на каждую найденную таблицу). Каждая ячейка содержит text, rowIndex,
    columnIndex — мы просто раскладываем их в матрицу и печатаем.

    Формат вывода для каждой таблицы:
        col0 | col1 | col2 | ...   ← каждая строка
        col0 | col1 | col2 | ...
    """
    try:
        tables = response["result"]["textAnnotation"].get("tables", []) or []
    except (KeyError, TypeError):
        return ""
    if not tables:
        return ""

    rendered_tables = []
    for tbl in tables:
        row_count = int(tbl.get("rowCount", 0) or 0)
        col_count = int(tbl.get("columnCount", 0) or 0)
        cells = tbl.get("cells", []) or []
        if row_count <= 0 or col_count <= 0 or not cells:
            continue

        matrix = [["" for _ in range(col_count)] for _ in range(row_count)]
        for c in cells:
            try:
                r = int(c.get("rowIndex", 0))
                co = int(c.get("columnIndex", 0))
            except (TypeError, ValueError):
                continue
            if not (0 <= r < row_count and 0 <= co < col_count):
                continue
            text = (c.get("text") or "").replace("\n", " ").strip()
            if matrix[r][co]:
                matrix[r][co] += " " + text
            else:
                matrix[r][co] = text

        rendered_tables.append(
            "\n".join(" | ".join(row) for row in matrix)
        )
    return "\n\n".join(rendered_tables)


def _render_fulltext_minus_tables(response: dict) -> str:
    """
    Возвращает fullText ответа — это полный распознанный текст документа,
    включающий и табличную часть, и всё остальное (шапка, поставщик,
    итоги, подписи). Для документов без таблиц — единственный источник.
    """
    try:
        return response["result"]["textAnnotation"].get("fullText", "") or ""
    except (KeyError, TypeError):
        return ""


async def yandex_read_text(image_bytes: bytes, model: str = "table") -> str:
    """
    Отправляет изображение в Yandex Vision OCR и возвращает распознанный
    текст. Если в ответе есть структурированные таблицы (tables[]), то
    вывод содержит эти таблицы построчно в pipe-separated виде + полный
    fullText документа как контекст. Это даёт LLM и чёткую табличную
    разметку позиций, и шапку с итогами.

    Автоматически определяет поворот: если первый OCR показывает
    вертикальный текст (median(w/h) < 1), картинка поворачивается на 90°
    и делается второй вызов.

    model:
        "table"           — для накладных/счетов/ТОРГ-12 (таблицы)
        "page"            — для обычного текста
        "handwritten"     — для рукописного текста (прайс-лист с Заказом)
        "page-column-sort"— для многоколоночных документов
    """
    # ── Первый вызов ────────────────────────────────────────────────────────────
    j = await _ocr_request(image_bytes, model)
    if "error" in j:
        err = j["error"]
        msg = err.get("message", str(err))
        logger.error("Yandex OCR error: %s", msg)
        raise Exception(f"Yandex OCR error: {msg}")

    lines = _extract_lines(j)

    # ── Автодетекция поворота ──────────────────────────────────────────────────
    # Если медианное w/h строк < 1 → текст вертикальный → картинка на боку.
    # Поворачиваем на 90° и делаем второй OCR. Стоит один лишний запрос.
    if lines:
        ratio = _median_aspect_ratio(lines)
        if ratio < 1.0:
            logger.info("Обнаружен повёрнутый текст (median w/h=%.2f), поворачиваю на 90°", ratio)
            try:
                from PIL import Image
                img = Image.open(io.BytesIO(image_bytes))
                rotated = img.rotate(90, expand=True)
                buf = io.BytesIO()
                rotated.save(buf, format="JPEG", quality=92)
                rotated_bytes = buf.getvalue()
                j2 = await _ocr_request(rotated_bytes, model)
                if "error" not in j2:
                    lines2 = _extract_lines(j2)
                    ratio2 = _median_aspect_ratio(lines2) if lines2 else 0
                    if ratio2 > ratio:
                        j = j2
                        lines = lines2
                        logger.info("Поворот успешен (median w/h=%.2f → %.2f)", ratio, ratio2)
                    else:
                        logger.warning("Поворот не помог (%.2f → %.2f), оставляю оригинал", ratio, ratio2)
            except Exception as e:
                logger.warning("Не удалось повернуть картинку: %s", e)

    # ── Парсим структурированные таблицы с bboxes ───────────────────────────────
    table_infos = _build_table_infos(j)
    full_text = _render_fulltext_minus_tables(j)
    handwritten_raw_text = ""

    # ── Второй проход для прайс-листов с рукописным заказом ────────────────────
    # Если в таблице есть колонка "Заказ" и она почти полностью пустая,
    # это прайс-лист где количество вписывается от руки. Запускаем
    # дополнительный вызов с model='handwritten' и по координатам ячеек
    # сопоставляем рукописные числа с товарами прямо в matrix.
    if table_infos and model != "handwritten":
        target_ti, order_col = _find_empty_order_table(table_infos)
        if target_ti is not None:
            logger.info("Обнаружена пустая колонка 'Заказ' → второй проход handwritten")
            try:
                # Если основной проход был по ротированной картинке — берём её же.
                # Мы не храним ротированные байты напрямую, но повторяем ту же
                # логику ротации, если в lines (извлечённых из j) обнаружится
                # нормальная ориентация. Проще всего: передать image_bytes и
                # положиться на то, что сам handwritten-проход тоже увидит
                # нормальный текст. Для Центропродукта картинка не ротирована.
                j_hw = await _ocr_request(image_bytes, "handwritten")
                if "error" not in j_hw:
                    filled = _fill_handwritten_orders(target_ti, order_col, j_hw)
                    logger.info("Заполнено %d ячеек в колонке Заказ", filled)
                    # Сохраняем также сырой handwritten-текст как fallback
                    # для LLM — если координатное сопоставление что-то упустит,
                    # LLM сможет подсмотреть рукописные пометки напрямую.
                    handwritten_raw_text = _render_fulltext_minus_tables(j_hw)
            except Exception as e:
                logger.warning("handwritten pass failed: %s", e)

    tables_text = _render_table_infos(table_infos) if table_infos else ""

    if tables_text:
        # Таблицы (возможно уже с заполненной колонкой Заказ) + fullText.
        parts = ["===== ТАБЛИЦЫ (структурированно) =====", tables_text]
        if full_text:
            parts += ["", "===== ПОЛНЫЙ ТЕКСТ ДОКУМЕНТА =====", full_text]
        if handwritten_raw_text:
            parts += [
                "",
                "===== ДОПОЛНИТЕЛЬНО: РУКОПИСНЫЕ ПОМЕТКИ =====",
                "Ниже распознан рукописный текст (для справки, если в таблице"
                " выше колонка 'Заказ' где-то не заполнена). Рукописный 'Итого'"
                " — это итоговая сумма заказа.",
                handwritten_raw_text,
            ]
        return "\n".join(parts)

    # Fallback: нет структурированных таблиц — отдаём fullText или
    # fallback-сортировку lines по (y, x).
    if full_text:
        return full_text

    if not lines:
        return ""
    lines.sort(key=lambda l: (l["y"], l["x"]))
    return "\n".join(l["text"] for l in lines)
