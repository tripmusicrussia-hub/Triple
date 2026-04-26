"""Git autopush для persistence на Render free disk (эфемерный).

Проблема: на Render free disk любой redeploy теряет файлы. `beats_data.json`
и `admin_prefs.json` критичны (юзер заполняет ключи через /quick_meta,
включает auto-repost), но без push'а в git они не переживают deploy.

Решение: background loop раз в N секунд проверяет dirty-файлы и делает
`git add+commit+push` через GitHub PAT. Push debounced — изменения за интервал
склеиваются в один коммит с сообщением `data: ...`.

ENV vars:
  GIT_AUTOPUSH_ENABLED   - "1" чтобы включить (default: off — локально не пушим)
  GIT_AUTOPUSH_TOKEN     - GitHub PAT с push scope (классический gho_*)
  GIT_AUTOPUSH_REMOTE    - origin URL без `https://`, например `github.com/owner/repo.git`
                            (default: парсим из `git remote get-url origin`)
  GIT_AUTOPUSH_BRANCH    - default "main"
  GIT_AUTOPUSH_USER_NAME - default "triple-bot-autopush"
  GIT_AUTOPUSH_USER_EMAIL- default "autopush@triplekillpost.bot"
  GIT_AUTOPUSH_INTERVAL  - сек между проверками (default 60)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Файлы помеченные как нуждающиеся в push. Ключ — relative path от repo root,
# значение — mtime когда mark_dirty был вызван (debug only).
_dirty: dict[str, float] = {}
_lock = asyncio.Lock()

_REPO_ROOT: Path | None = None  # вычисляем lazy


def is_enabled() -> bool:
    return os.getenv("GIT_AUTOPUSH_ENABLED", "0") == "1"


def _repo_root() -> Path:
    global _REPO_ROOT
    if _REPO_ROOT is not None:
        return _REPO_ROOT
    here = Path(__file__).resolve().parent
    # bot-assistant/bot-assistant/git_autopush.py → repo root = parents[2]
    _REPO_ROOT = here.parent.parent
    return _REPO_ROOT


def mark_dirty(filepath: str | Path) -> None:
    """Отметить файл как изменённый — будет push'нут в следующий tick цикла.

    Безопасно вызывать из любого потока (sync). Idempotent: повторные mark
    за интервал → один коммит.
    """
    try:
        p = Path(filepath).resolve()
        rel = str(p.relative_to(_repo_root())).replace("\\", "/")
        _dirty[rel] = time.time()
    except Exception:
        logger.warning("git_autopush.mark_dirty failed for %s", filepath, exc_info=True)


def _build_remote_url(token: str) -> str | None:
    """Подставляет PAT в origin URL — для push'а без интерактивного auth."""
    explicit = os.getenv("GIT_AUTOPUSH_REMOTE", "").strip()
    if explicit:
        host_path = explicit
    else:
        try:
            res = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                cwd=str(_repo_root()),
                capture_output=True, text=True, timeout=5,
            )
            if res.returncode != 0:
                logger.warning("autopush: git remote get-url failed: %s", res.stderr.strip())
                return None
            url = res.stdout.strip()
        except Exception:
            logger.exception("autopush: failed to read origin url")
            return None
        # Нормализуем:
        #   https://github.com/owner/repo.git → github.com/owner/repo.git
        #   git@github.com:owner/repo.git    → github.com/owner/repo.git
        m = re.match(r"https?://(?:[^@]+@)?(.+)", url)
        if m:
            host_path = m.group(1)
        else:
            m = re.match(r"git@([^:]+):(.+)", url)
            if not m:
                logger.warning("autopush: cannot parse origin url %s", url)
                return None
            host_path = f"{m.group(1)}/{m.group(2)}"
    return f"https://x-access-token:{token}@{host_path}"


async def _run_git(args: list[str], cwd: str, env: dict | None = None,
                   timeout: int = 30) -> tuple[int, str, str]:
    """async git wrapper. Возвращает (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        "git", *args,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        return 1, "", f"git {args[0]} timeout"
    return (
        proc.returncode or 0,
        (stdout_b or b"").decode("utf-8", errors="replace"),
        (stderr_b or b"").decode("utf-8", errors="replace"),
    )


async def _do_push_once() -> int:
    """Один проход: если есть dirty файлы → add+commit+push. Возвращает кол-во
    закоммиченных файлов (0 если nothing-to-commit или ошибка)."""
    async with _lock:
        if not _dirty:
            return 0
        files = sorted(_dirty.keys())
        _dirty.clear()  # snapshot — если save во время push, попадёт в next tick

    token = os.getenv("GIT_AUTOPUSH_TOKEN", "").strip()
    if not token:
        logger.warning("autopush: GIT_AUTOPUSH_TOKEN не задан, skip push для %s", files)
        return 0

    remote_url = _build_remote_url(token)
    if not remote_url:
        return 0

    branch = os.getenv("GIT_AUTOPUSH_BRANCH", "main").strip() or "main"
    user_name = os.getenv("GIT_AUTOPUSH_USER_NAME", "triple-bot-autopush").strip()
    user_email = os.getenv("GIT_AUTOPUSH_USER_EMAIL", "autopush@triplekillpost.bot").strip()

    repo = str(_repo_root())
    env = os.environ.copy()
    # Явно ставим committer identity для этого процесса (не глобально через config)
    env["GIT_AUTHOR_NAME"] = user_name
    env["GIT_AUTHOR_EMAIL"] = user_email
    env["GIT_COMMITTER_NAME"] = user_name
    env["GIT_COMMITTER_EMAIL"] = user_email

    # Pull first, чтобы избежать non-fast-forward (на Render может быть slightly
    # outdated если кто-то push'ил с моей машины). --rebase + --autostash чтобы
    # текущие dirty уже в working tree (мы их add'нем дальше).
    rc, out, err = await _run_git(["pull", "--rebase", "--autostash", "origin", branch],
                                  cwd=repo, env=env, timeout=30)
    if rc != 0:
        logger.warning("autopush: git pull failed (rc=%d): %s", rc, err.strip()[-300:])
        # Продолжаем — push может всё равно пройти если ничего не сменилось

    # Add только наши файлы (не trash, не временные)
    rc, out, err = await _run_git(["add", "--", *files], cwd=repo, env=env, timeout=15)
    if rc != 0:
        logger.warning("autopush: git add failed (rc=%d): %s", rc, err.strip())
        # Re-mark файлы как dirty (попробуем в следующий tick)
        for f in files:
            _dirty[f] = time.time()
        return 0

    # Сообщение коммита
    short_files = ", ".join(Path(f).name for f in files)
    commit_msg = f"data(autopush): {short_files}"
    rc, out, err = await _run_git(["commit", "-m", commit_msg], cwd=repo, env=env, timeout=15)
    if rc != 0:
        # nothing to commit — нормально (файл add'нули, но git считает no diff)
        if "nothing to commit" in (out + err).lower():
            logger.info("autopush: no diff for %s, skip", files)
            return 0
        logger.warning("autopush: git commit failed (rc=%d): %s", rc, err.strip())
        return 0

    # Push с подменой URL — temporary remote
    rc, out, err = await _run_git(
        ["push", remote_url, f"HEAD:{branch}"],
        cwd=repo, env=env, timeout=60,
    )
    if rc != 0:
        # Sanitize stderr перед логом — НЕ должен светиться token (он внутри URL)
        safe_err = err.replace(token, "***")[-300:]
        logger.error("autopush: git push failed (rc=%d): %s", rc, safe_err)
        # На Render следующий tick попробует ещё раз — local commit остался,
        # просто не зарегистрировался на remote. Повторный mark не нужен.
        return 0

    logger.info("autopush: pushed %d file(s) to %s: %s", len(files), branch, short_files)
    return len(files)


async def autopush_loop():
    """Background loop: каждые `interval_sec` проверяет dirty и пушит.

    Запускается из post_init. На Render free tier работает только когда bot
    awake (но active дольше idle — TG webhook + scheduler tasks).
    """
    if not is_enabled():
        logger.info("autopush: disabled (GIT_AUTOPUSH_ENABLED != 1), loop not started")
        return
    interval = int(os.getenv("GIT_AUTOPUSH_INTERVAL", "60"))
    logger.info("autopush: loop started, interval=%ds", interval)
    while True:
        try:
            await asyncio.sleep(interval)
            n = await _do_push_once()
            if n:
                logger.info("autopush: tick pushed %d file(s)", n)
        except Exception:
            logger.exception("autopush: loop iteration crashed (continuing)")
