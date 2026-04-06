import json
import os
import httpx
from datetime import datetime, timedelta

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_WHISPER_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
MODEL = "llama-3.3-70b-versatile"
GROQ_KEY = os.getenv("GROQ_API_KEY", "")

DATA_FILE = "assistant_data.json"

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                # Ensure all keys exist
                for key in ["expenses","income","tasks","notes","contacts","reminders","goals"]:
                    if key not in d:
                        d[key] = []
                return d
        except Exception:
            pass
    return {"expenses":[],"income":[],"tasks":[],"notes":[],"contacts":[],"reminders":[],"goals":[]}

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_stats(data, period="month"):
    now = datetime.now()
    if period == "today":
        since = now.strftime("%Y-%m-%d")
    elif period == "week":
        since = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    elif period == "month":
        since = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    else:
        since = "2000-01-01"

    expenses = [e for e in data["expenses"] if e.get("date","") >= since]
    income = [i for i in data["income"] if i.get("date","") >= since]

    total_exp = sum(e.get("amount",0) for e in expenses)
    total_inc = sum(i.get("amount",0) for i in income)

    by_cat = {}
    for e in expenses:
        cat = e.get("category","другое")
        by_cat[cat] = by_cat.get(cat, 0) + e.get("amount", 0)

    lines = [f"📊 Статистика за {{'today':'сегодня','week':'неделю','month':'месяц','all':'всё время'}[period]}:"]
    lines.append(f"💚 Доходы: {total_inc:,.0f} ₽")
    lines.append(f"❤️ Расходы: {total_exp:,.0f} ₽")
    lines.append(f"💰 Баланс: {total_inc-total_exp:,.0f} ₽")
    if by_cat:
        lines.append("\nПо категориям:")
        for cat, amt in sorted(by_cat.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {amt:,.0f} ₽")
    return "\n".join(lines)

def get_daily_report(data):
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    since_month = (now - timedelta(days=30)).strftime("%Y-%m-%d")

    total_inc = sum(i.get("amount",0) for i in data["income"] if i.get("date","") >= since_month)
    total_exp = sum(e.get("amount",0) for e in data["expenses"] if e.get("date","") >= since_month)
    balance = total_inc - total_exp

    pending_tasks = [t for t in data["tasks"] if not t.get("done")]
    today_tasks = [t for t in pending_tasks if t.get("deadline","") == today]
    overdue = [t for t in pending_tasks if t.get("deadline","") and t.get("deadline","") < today]

    lines = [f"☀️ Добрый день! Вот твой отчёт на {now.strftime('%d.%m.%Y')}:\n"]
    lines.append(f"💰 Баланс за месяц: {balance:,.0f} ₽")
    lines.append(f"   Доходы: {total_inc:,.0f} ₽ | Расходы: {total_exp:,.0f} ₽\n")

    if today_tasks:
        lines.append("📌 Задачи на сегодня:")
        for t in today_tasks[:5]:
            lines.append(f"  • {t['title']}")
        lines.append("")

    if overdue:
        lines.append("⚠️ Просроченные задачи:")
        for t in overdue[:3]:
            lines.append(f"  • {t['title']} (до {t.get('deadline','')})")
        lines.append("")

    if pending_tasks and not today_tasks:
        lines.append(f"✅ Активных задач: {len(pending_tasks)}")

    if data.get("goals"):
        lines.append("\n🎯 Цели:")
        for g in data["goals"][:3]:
            pct = int(g.get("saved",0) / g.get("target",1) * 100) if g.get("target") else 0
            bar = "█" * (pct//10) + "░" * (10 - pct//10)
            lines.append(f"  {g['title']}: {g.get('saved',0):,.0f}/{g.get('target',0):,.0f} ₽ [{bar}] {pct}%")

    return "\n".join(lines)

def get_summary():
    data = load_data()
    total_inc = sum(x.get("amount",0) for x in data["income"])
    total_exp = sum(x.get("amount",0) for x in data["expenses"])
    balance = total_inc - total_exp
    pending = [t for t in data["tasks"] if not t.get("done")]

    lines = [
        f"💰 Баланс: {balance:,.0f} ₽ (доходы {total_inc:,.0f} − расходы {total_exp:,.0f})",
        f"✅ Активных задач: {len(pending)}",
        f"📝 Заметок: {len(data['notes'])}",
        f"👤 Контактов: {len(data['contacts'])}",
    ]
    if data.get("goals"):
        lines.append(f"🎯 Целей: {len(data['goals'])}")
    if pending:
        lines.append("\nАктивные задачи:")
        for t in pending[:5]:
            dl = f" (до {t['deadline']})" if t.get("deadline") else ""
            lines.append(f"  • {t['title']}{dl}")
    return "\n".join(lines)

def system_prompt(data):
    today = datetime.now().strftime("%A, %d %B %Y, %H:%M")
    summary = {
        "расходы (последние 20)": data["expenses"][-20:],
        "доходы (последние 20)": data["income"][-20:],
        "задачи": [t for t in data["tasks"] if not t.get("done")][-20:],
        "заметки": data["notes"][-10:],
        "контакты": data["contacts"][-20:],
        "напоминания": [r for r in data["reminders"] if not r.get("done")][-10:],
        "цели": data.get("goals",[])[-10:],
    }
    return f"""Ты личный ассистент. Сегодня {today}.

ДАННЫЕ ПОЛЬЗОВАТЕЛЯ:
{json.dumps(summary, ensure_ascii=False)}

Верни ТОЛЬКО JSON без markdown:
{{"action":"add_expense"|"add_income"|"add_task"|"add_note"|"add_contact"|"add_reminder"|"add_goal"|"update_goal"|"complete_task"|"delete"|"stats"|"query"|"none","data":{{...}},"response":"текст ответа по-русски"}}

ДЕЙСТВИЯ:
- add_expense: {{amount:число, category:"еда"|"транспорт"|"здоровье"|"развлечения"|"покупки"|"другое", description:"текст"}}
- add_income: {{amount:число, source:"зарплата"|"фриланс"|"другое", description:"текст"}}
- add_task: {{title:"текст", deadline:"YYYY-MM-DD или null", priority:"high"|"medium"|"low"}}
- add_note: {{title:"текст", content:"текст"}}
- add_contact: {{name:"текст", phone:"или null", notes:"или null"}}
- add_reminder: {{text:"текст", datetime:"YYYY-MM-DDTHH:MM"}}
- add_goal: {{title:"текст", target:число, saved:число}}
- update_goal: {{id:число, saved:число}}
- complete_task: {{id:число}}
- delete: {{type:"expense"|"income"|"task"|"note"|"contact"|"goal", id:число}}
- stats: {{period:"today"|"week"|"month"|"all"}}
- query/none: {{}}

СТИЛЬ: очень коротко, 1-2 предложения. Суммы в рублях."""

async def transcribe_voice(file_bytes: bytes, filename: str = "voice.ogg") -> str:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GROQ_WHISPER_URL,
            headers={"Authorization": f"Bearer {GROQ_KEY}"},
            files={"file": (filename, file_bytes, "audio/ogg")},
            data={"model": "whisper-large-v3", "language": "ru"}
        )
        return resp.json().get("text", "")

async def process_message(text: str, conversation_history: list) -> dict:
    data = load_data()
    messages = [{"role": "system", "content": system_prompt(data)}]
    messages += conversation_history[-8:]
    messages.append({"role": "user", "content": text})

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL, "max_tokens": 500, "messages": messages}
        )
        j = resp.json()
        if "error" in j:
            raise Exception(j["error"]["message"])
        raw = j["choices"][0]["message"]["content"]

    parsed = {"action": "none", "data": {}, "response": raw}
    try:
        parsed = json.loads(raw.replace("```json","").replace("```","").strip())
    except Exception:
        pass

    # Handle stats action
    if parsed.get("action") == "stats":
        period = parsed.get("data", {}).get("period", "month")
        parsed["response"] = get_stats(data, period)
        return parsed

    apply_action(parsed, data)
    return parsed

def apply_action(parsed: dict, data: dict):
    now = datetime.now().strftime("%Y-%m-%d")
    uid = int(datetime.now().timestamp() * 1000)
    action = parsed.get("action", "none")
    d = parsed.get("data", {})

    if action == "add_expense" and d.get("amount"):
        data["expenses"].append({"id": uid, "date": now, **d})
        save_data(data)
    elif action == "add_income" and d.get("amount"):
        data["income"].append({"id": uid, "date": now, **d})
        save_data(data)
    elif action == "add_task" and d.get("title"):
        data["tasks"].append({"id": uid, "done": False, "created": now, **d})
        save_data(data)
    elif action == "add_note" and d.get("title"):
        data["notes"].append({"id": uid, "created": now, **d})
        save_data(data)
    elif action == "add_contact" and d.get("name"):
        data["contacts"].append({"id": uid, **d})
        save_data(data)
    elif action == "add_reminder" and d.get("text"):
        data["reminders"].append({"id": uid, "done": False, **d})
        save_data(data)
    elif action == "add_goal" and d.get("title"):
        data["goals"].append({"id": uid, "title": d["title"], "target": d.get("target", 0), "saved": d.get("saved", 0)})
        save_data(data)
    elif action == "update_goal" and d.get("id"):
        for g in data["goals"]:
            if g["id"] == d["id"]:
                g["saved"] = d.get("saved", g.get("saved", 0))
        save_data(data)
    elif action == "complete_task" and d.get("id"):
        for t in data["tasks"]:
            if t["id"] == d["id"]:
                t["done"] = True
        save_data(data)
    elif action == "delete" and d.get("type") and d.get("id"):
        cat_map = {"expense":"expenses","income":"income","task":"tasks","note":"notes","contact":"contacts","goal":"goals"}
        cat = cat_map.get(d["type"])
        if cat and cat in data:
            data[cat] = [x for x in data[cat] if x["id"] != d["id"]]
            save_data(data)
