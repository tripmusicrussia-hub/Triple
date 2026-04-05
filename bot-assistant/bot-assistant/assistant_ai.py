import json
import os
import httpx
from datetime import datetime

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_WHISPER_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
MODEL = "llama-3.3-70b-versatile"
GROQ_KEY = os.getenv("GROQ_API_KEY", "")

DATA_FILE = "assistant_data.json"

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"expenses": [], "income": [], "tasks": [], "notes": [], "contacts": [], "reminders": []}

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def system_prompt(data):
    today = datetime.now().strftime("%A, %d %B %Y, %H:%M")
    summary = {
        "расходы (последние 20)": data["expenses"][-20:],
        "доходы (последние 20)": data["income"][-20:],
        "задачи": [t for t in data["tasks"] if not t.get("done")][-20:],
        "заметки": data["notes"][-10:],
        "контакты": data["contacts"][-20:],
        "напоминания": [r for r in data["reminders"] if not r.get("done")][-10:],
    }
    return f"""Ты личный ассистент. Сегодня {today}.

ДАННЫЕ ПОЛЬЗОВАТЕЛЯ:
{json.dumps(summary, ensure_ascii=False)}

Верни ТОЛЬКО JSON без markdown:
{{"action":"add_expense"|"add_income"|"add_task"|"add_note"|"add_contact"|"add_reminder"|"complete_task"|"delete"|"query"|"none","data":{{...}},"response":"текст ответа по-русски"}}

ДЕЙСТВИЯ И ПОЛЯ:
- add_expense: {{amount:число, category:"еда"|"транспорт"|"здоровье"|"развлечения"|"покупки"|"другое", description:"текст"}}
- add_income: {{amount:число, source:"зарплата"|"фриланс"|"другое", description:"текст"}}
- add_task: {{title:"текст", deadline:"YYYY-MM-DD или null", priority:"high"|"medium"|"low"}}
- add_note: {{title:"текст", content:"текст"}}
- add_contact: {{name:"текст", phone:"или null", notes:"или null"}}
- add_reminder: {{text:"текст", datetime:"YYYY-MM-DDTHH:MM"}}
- complete_task: {{id:число}}
- delete: {{type:"expense"|"income"|"task"|"note"|"contact", id:число}}
- query: {{}} — отвечай в response
- none: {{}} — обычный разговор

СТИЛЬ: очень коротко, 1-2 предложения. Суммы в рублях."""

async def transcribe_voice(file_bytes: bytes, filename: str = "voice.ogg") -> str:
    """Transcribe voice message using Groq Whisper"""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            GROQ_WHISPER_URL,
            headers={"Authorization": f"Bearer {GROQ_KEY}"},
            files={"file": (filename, file_bytes, "audio/ogg")},
            data={"model": "whisper-large-v3", "language": "ru"}
        )
        j = resp.json()
        return j.get("text", "")

async def process_message(text: str, conversation_history: list) -> dict:
    """Send message to Groq and get structured response"""
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
        parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())
    except Exception:
        pass

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

    elif action == "complete_task" and d.get("id"):
        for t in data["tasks"]:
            if t["id"] == d["id"]:
                t["done"] = True
        save_data(data)

    elif action == "delete" and d.get("type") and d.get("id"):
        cat_map = {"expense": "expenses", "income": "income", "task": "tasks",
                   "note": "notes", "contact": "contacts"}
        cat = cat_map.get(d["type"])
        if cat and cat in data:
            data[cat] = [x for x in data[cat] if x["id"] != d["id"]]
            save_data(data)

def get_summary() -> str:
    """Return a text summary of all data"""
    data = load_data()
    total_inc = sum(x.get("amount", 0) for x in data["income"])
    total_exp = sum(x.get("amount", 0) for x in data["expenses"])
    balance = total_inc - total_exp
    pending_tasks = [t for t in data["tasks"] if not t.get("done")]

    lines = [
        f"💰 Баланс: {balance:,.0f} ₽ (доходы {total_inc:,.0f} − расходы {total_exp:,.0f})",
        f"✅ Задач активных: {len(pending_tasks)}",
        f"📝 Заметок: {len(data['notes'])}",
        f"👤 Контактов: {len(data['contacts'])}",
    ]
    if pending_tasks:
        lines.append("\nАктивные задачи:")
        for t in pending_tasks[-5:]:
            dl = f" (до {t['deadline']})" if t.get("deadline") else ""
            lines.append(f"  • {t['title']}{dl}")
    return "\n".join(lines)
