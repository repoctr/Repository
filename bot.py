#!/usr/bin/env python3
"""Telegram bot: connect GitHub, upload files, create multiple repos, update .env."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import shutil
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Lock
from typing import Any

import httpx
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("repoforge")

# --- GitHub API (inlined) ---
API = "https://api.github.com"
API_VERSION = "2022-11-28"
USER_AGENT = "RepoForge-TelegramBot"


class GitHubError(Exception):
    def __init__(self, message: str, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": USER_AGENT,
    }


def _error_message(resp: httpx.Response) -> str:
    try:
        data = resp.json()
        msg = data.get("message") or resp.reason_phrase
        extra = data.get("errors")
        if extra:
            details = "; ".join(
                e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in extra
            )
            return f"{msg} ({details})"
        return str(msg)
    except Exception:
        return resp.text[:400] or resp.reason_phrase


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return resp.text[:400]


class GitHubClient:
    def __init__(self, token: str, timeout: float = 60.0):
        if not token or not token.strip():
            raise GitHubError("GitHub token is empty.")
        self.token = token.strip()
        self._timeout = timeout
        self._owner: str | None = None

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers=_headers(self.token),
            timeout=self._timeout,
            follow_redirects=True,
        )

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = path if path.startswith("http") else f"{API}{path}"
        last_error: GitHubError | None = None
        async with self._client() as client:
            for attempt in range(6):
                resp = await client.request(method, url, **kwargs)
                retryable = resp.status_code in {429, 502, 503} or (
                    resp.status_code == 403
                    and "rate limit" in (resp.text or "").lower()
                )
                if retryable and attempt < 5:
                    retry_after = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
                    try:
                        wait = float(retry_after) if retry_after else 1.2 * (attempt + 1)
                    except ValueError:
                        wait = 1.2 * (attempt + 1)
                    wait = min(max(wait, 0.8), 25)
                    logger.warning(
                        "GitHub %s on %s %s — retry %s in %.1fs",
                        resp.status_code,
                        method,
                        path,
                        attempt + 1,
                        wait,
                    )
                    await asyncio.sleep(wait)
                    last_error = GitHubError(
                        _error_message(resp),
                        status=resp.status_code,
                        body=_safe_json(resp),
                    )
                    continue
                if resp.status_code >= 400:
                    raise GitHubError(
                        _error_message(resp),
                        status=resp.status_code,
                        body=_safe_json(resp),
                    )
                return resp
        raise last_error or GitHubError("GitHub request failed after retries.")

    async def get_owner(self) -> str:
        if self._owner:
            return self._owner
        resp = await self._request("GET", "/user")
        data = resp.json()
        login = data.get("login")
        if not login:
            raise GitHubError("Could not read GitHub username from token.")
        self._owner = login
        return login

    async def list_repos(self, max_repos: int = 100) -> list[dict[str, Any]]:
        """List repositories owned by the authenticated user."""
        repos: list[dict[str, Any]] = []
        page = 1
        per_page = 100
        while len(repos) < max_repos:
            resp = await self._request(
                "GET",
                "/user/repos",
                params={
                    "per_page": per_page,
                    "page": page,
                    "affiliation": "owner",
                    "sort": "updated",
                    "direction": "desc",
                },
            )
            batch = resp.json()
            if not isinstance(batch, list) or not batch:
                break
            for item in batch:
                owner = (item.get("owner") or {}).get("login") or ""
                repos.append(
                    {
                        "name": item.get("name") or "",
                        "full_name": item.get("full_name") or f"{owner}/{item.get('name')}",
                        "url": item.get("html_url") or "",
                        "private": bool(item.get("private")),
                        "owner": owner,
                    }
                )
                if len(repos) >= max_repos:
                    break
            if len(batch) < per_page:
                break
            page += 1
        return repos

    async def repo_exists(self, name: str) -> bool:
        owner = await self.get_owner()
        url = f"{API}/repos/{owner}/{name}"
        async with self._client() as client:
            resp = await client.get(url)
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        raise GitHubError(_error_message(resp), status=resp.status_code, body=_safe_json(resp))

    async def create_repo(self, name: str, private: bool, description: str = "") -> dict[str, Any]:
        # auto_init so the Git database exists and we can commit files.
        payload: dict[str, Any] = {
            "name": name,
            "private": private,
            "auto_init": True,
            "has_issues": True,
            "has_projects": False,
            "has_wiki": False,
        }
        if description:
            payload["description"] = description
        resp = await self._request("POST", "/user/repos", json=payload)
        return resp.json()

    async def _default_branch(self, owner: str, repo: str) -> str:
        resp = await self._request("GET", f"/repos/{owner}/{repo}")
        return resp.json().get("default_branch") or "main"

    async def _wait_for_branch(self, owner: str, repo: str, attempts: int = 8) -> str:
        last_error: Exception | None = None
        for i in range(attempts):
            try:
                branch = await self._default_branch(owner, repo)
                await self._request("GET", f"/repos/{owner}/{repo}/git/ref/heads/{branch}")
                return branch
            except GitHubError as exc:
                last_error = exc
                await asyncio.sleep(0.4 * (i + 1))
        raise GitHubError(
            f"Repository {repo} was created but the default branch is not ready yet. "
            f"{last_error or ''}"
        )

    async def push_files(
        self,
        repo: str,
        files: dict[str, bytes],
        message: str = "Add project files",
    ) -> str:
        """Replace the repo tree with the uploaded files (single commit). Returns commit SHA."""
        if not files:
            raise GitHubError("No files to push.")

        owner = await self.get_owner()
        try:
            return await self._push_via_git_data(owner, repo, files, message)
        except GitHubError as exc:
            logger.warning("Git Data API push failed for %s (%s). Falling back to Contents API.", repo, exc)
            await self._push_via_contents(owner, repo, files, message)
            return ""

    async def _push_via_git_data(
        self,
        owner: str,
        repo: str,
        files: dict[str, bytes],
        message: str,
    ) -> str:
        branch = await self._wait_for_branch(owner, repo)
        ref = await self._request("GET", f"/repos/{owner}/{repo}/git/ref/heads/{branch}")
        parent_sha = ref.json()["object"]["sha"]

        tree_items: list[dict[str, str]] = []
        for path, content in files.items():
            blob_sha = await self._create_blob(owner, repo, content)
            tree_items.append(
                {
                    "path": path.lstrip("/"),
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_sha,
                }
            )

        # No base_tree → the new commit contains only the uploaded files
        # (the auto-generated README is dropped unless the user uploaded one).
        tree_sha = await self._create_tree(owner, repo, tree_items)
        commit_sha = await self._create_commit(
            owner, repo, message, tree_sha, parents=[parent_sha]
        )
        await self._update_ref(owner, repo, branch, commit_sha)
        return commit_sha

    async def _push_via_contents(
        self,
        owner: str,
        repo: str,
        files: dict[str, bytes],
        message: str,
    ) -> None:
        branch = "main"
        try:
            branch = await self._default_branch(owner, repo)
        except GitHubError:
            pass
        first = True
        for path, content in files.items():
            msg = message if first else f"Add {path}"
            await self.put_file(repo, path, content, msg, branch=branch)
            first = False
            await asyncio.sleep(0.2)

    async def get_file(self, repo: str, path: str, branch: str | None = None) -> tuple[bytes, str] | None:
        owner = await self.get_owner()
        if not branch:
            try:
                branch = await self._default_branch(owner, repo)
            except GitHubError:
                branch = "main"
        url = f"{API}/repos/{owner}/{repo}/contents/{path.lstrip('/')}"
        async with self._client() as client:
            resp = await client.get(url, params={"ref": branch})
        if resp.status_code == 404:
            return None
        if resp.status_code >= 400:
            raise GitHubError(_error_message(resp), status=resp.status_code, body=_safe_json(resp))
        data = resp.json()
        if isinstance(data, list) or data.get("type") != "file":
            raise GitHubError(f"{path} is not a file in {repo}.")
        content_b64 = (data.get("content") or "").replace("\n", "")
        encoding = data.get("encoding")
        if encoding == "base64" or content_b64:
            try:
                raw = base64.b64decode(content_b64)
            except Exception as exc:
                raise GitHubError(f"Could not decode {path}: {exc}") from exc
        else:
            raise GitHubError(f"Could not read contents of {path}.")
        sha = data.get("sha")
        if not sha:
            raise GitHubError(f"GitHub did not return a SHA for {path}.")
        return raw, sha

    async def put_file(
        self,
        repo: str,
        path: str,
        content: bytes,
        message: str,
        sha: str | None = None,
        branch: str = "main",
    ) -> dict[str, Any]:
        owner = await self.get_owner()
        payload: dict[str, Any] = {
            "message": message,
            "content": base64.b64encode(content).decode("ascii"),
            "branch": branch,
        }
        if sha:
            payload["sha"] = sha
        resp = await self._request(
            "PUT",
            f"/repos/{owner}/{repo}/contents/{path.lstrip('/')}",
            json=payload,
        )
        return resp.json()

    async def upsert_env_key(self, repo: str, key: str, value: str) -> str:
        """Set KEY=value in .env (create the file if missing)."""
        owner = await self.get_owner()
        try:
            branch = await self._default_branch(owner, repo)
        except GitHubError:
            branch = "main"

        existing = await self.get_file(repo, ".env", branch=branch)
        if existing is None:
            text = f"{key}={value}\n"
            await self.put_file(
                repo,
                ".env",
                text.encode("utf-8"),
                message=f"Create .env and set {key}",
                branch=branch,
            )
            return "created"
        raw, sha = existing
        try:
            original = raw.decode("utf-8")
        except UnicodeDecodeError:
            original = raw.decode("utf-8", errors="replace")
        updated = upsert_env_line(original, key, value)
        if updated == original:
            return "unchanged"
        await self.put_file(
            repo,
            ".env",
            updated.encode("utf-8"),
            message=f"Update {key} in .env",
            sha=sha,
            branch=branch,
        )
        return "updated"

    async def _create_blob(self, owner: str, repo: str, content: bytes) -> str:
        resp = await self._request(
            "POST",
            f"/repos/{owner}/{repo}/git/blobs",
            json={
                "content": base64.b64encode(content).decode("ascii"),
                "encoding": "base64",
            },
        )
        sha = resp.json().get("sha")
        if not sha:
            raise GitHubError("GitHub did not return a blob SHA.")
        return sha

    async def _create_tree(self, owner: str, repo: str, tree: list[dict[str, str]]) -> str:
        resp = await self._request(
            "POST",
            f"/repos/{owner}/{repo}/git/trees",
            json={"tree": tree},
        )
        sha = resp.json().get("sha")
        if not sha:
            raise GitHubError("GitHub did not return a tree SHA.")
        return sha

    async def _create_commit(
        self,
        owner: str,
        repo: str,
        message: str,
        tree_sha: str,
        parents: list[str] | None = None,
    ) -> str:
        payload: dict[str, Any] = {"message": message, "tree": tree_sha}
        if parents:
            payload["parents"] = parents
        resp = await self._request(
            "POST",
            f"/repos/{owner}/{repo}/git/commits",
            json=payload,
        )
        sha = resp.json().get("sha")
        if not sha:
            raise GitHubError("GitHub did not return a commit SHA.")
        return sha

    async def _update_ref(self, owner: str, repo: str, branch: str, sha: str) -> None:
        await self._request(
            "PATCH",
            f"/repos/{owner}/{repo}/git/refs/heads/{branch}",
            json={"sha": sha, "force": True},
        )

    async def update_files(self, repo: str, files: dict[str, bytes]) -> list[str]:
        notes: list[str] = []
        owner = await self.get_owner()
        try:
            branch = await self._default_branch(owner, repo)
        except GitHubError:
            branch = "main"
        for path, content in files.items():
            existing = await self.get_file(repo, path, branch=branch)
            if existing is None:
                await self.put_file(repo, path, content, f"Add {path}", branch=branch)
                notes.append(f"added {path}")
                continue
            raw, sha = existing
            if raw == content:
                notes.append(f"unchanged {path}")
                continue
            await self.put_file(
                repo, path, content, f"Update {path}", sha=sha, branch=branch
            )
            notes.append(f"updated {path}")
        return notes


def upsert_env_line(text: str, key: str, value: str) -> str:
    newline = "\r\n" if "\r\n" in text else "\n"
    ended_with_nl = text.endswith("\n")
    lines = text.splitlines()
    found = False
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            out.append(line)
            continue
        name, sep, _rest = stripped.partition("=")
        if sep and name.strip() == key:
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"{key}={value}")
        ended_with_nl = True
    result = newline.join(out)
    if ended_with_nl:
        result += newline
    return result

# --- Storage (inlined) ---
DATA_DIR = Path(__file__).resolve().parent / "data"
USERS_FILE = DATA_DIR / "users.json"
_LOCK = Lock()


def _empty() -> dict[str, Any]:
    return {}


def _load() -> dict[str, Any]:
    if not USERS_FILE.exists():
        return _empty()
    try:
        with USERS_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
    except Exception:
        logger.exception("Failed to read %s", USERS_FILE)
    return _empty()


def _save(data: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = USERS_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    tmp.replace(USERS_FILE)


def _user(data: dict[str, Any], user_id: int) -> dict[str, Any]:
    key = str(user_id)
    user = data.get(key)
    if not isinstance(user, dict):
        user = {}
        data[key] = user
    return user


def set_github_token(user_id: int, token: str, owner: str | None = None) -> None:
    with _LOCK:
        data = _load()
        user = _user(data, user_id)
        user["github_token"] = token.strip()
        if owner:
            user["github_owner"] = owner
        _save(data)


def get_github_token(user_id: int) -> str | None:
    with _LOCK:
        data = _load()
        user = data.get(str(user_id)) or {}
        token = user.get("github_token")
        if isinstance(token, str) and token.strip():
            return token.strip()
        return None


def get_github_owner(user_id: int) -> str | None:
    with _LOCK:
        data = _load()
        user = data.get(str(user_id)) or {}
        owner = user.get("github_owner")
        if isinstance(owner, str) and owner.strip():
            return owner.strip()
        return None


def set_numbered_repos(user_id: int, repos: list[dict[str, Any]]) -> None:
    """Numbered list used by /up .envN — last create batch or last /getrepo."""
    with _LOCK:
        data = _load()
        user = _user(data, user_id)
        user["last_batch"] = repos
        _save(data)


def clear_github_token(user_id: int) -> None:
    with _LOCK:
        data = _load()
        user = data.get(str(user_id))
        if isinstance(user, dict) and "github_token" in user:
            user.pop("github_token", None)
            _save(data)


def save_batch(user_id: int, repos: list[dict[str, Any]]) -> None:
    with _LOCK:
        data = _load()
        user = _user(data, user_id)
        user["last_batch"] = repos
        history = user.get("history")
        if not isinstance(history, list):
            history = []
        history.extend(repos)
        user["history"] = history[-200:]
        _save(data)


def get_last_batch(user_id: int) -> list[dict[str, Any]]:
    with _LOCK:
        data = _load()
        user = data.get(str(user_id)) or {}
        batch = user.get("last_batch")
        if isinstance(batch, list):
            return batch
        return []


def get_repo_by_index(user_id: int, index: int) -> dict[str, Any] | None:
    for repo in get_last_batch(user_id):
        if isinstance(repo, dict) and int(repo.get("index", 0)) == index:
            return repo
    return None



BOT_TOKEN = (
    os.getenv("BOT_TOKEN", "").strip()
    or "8256576860:AAH-lABEcy2Wh41b4pHmdM_T7oBBMftsb_I"
)
OWNER_ID = 5206554804
ALLOWED_FILE = DATA_DIR / "allowed.json"


def _allowed_paths() -> list[Path]:
    return [DATA_DIR / "allowed.json", Path("/tmp/repoforge-data") / "allowed.json"]


def _load_extra_allowed() -> set[int]:
    ids: set[int] = set()
    for path in _allowed_paths():
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                ids.update(int(x) for x in raw)
        except Exception:
            logger.exception("Failed to read %s", path)
    return ids


def _save_extra_allowed(ids: set[int]) -> None:
    payload = json.dumps(sorted(ids), indent=2) + "\n"
    last: Exception | None = None
    for folder in (DATA_DIR, Path("/tmp/repoforge-data")):
        try:
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / "allowed.json"
            path.write_text(payload, encoding="utf-8")
            global ALLOWED_FILE
            ALLOWED_FILE = path
            return
        except Exception as exc:
            last = exc
            logger.warning("Could not write allowed list to %s: %s", folder, exc)
    raise OSError(f"Could not save allowed users: {last}")


def extra_allowed_ids() -> set[int]:
    return _load_extra_allowed()


def all_allowed_ids() -> set[int]:
    return {OWNER_ID} | extra_allowed_ids()

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_ROOT = BASE_DIR / "data" / "uploads"

WAIT_TOKEN, WAIT_FILES, WAIT_QTY, WAIT_VIS, WAIT_NAME, WAIT_EDIT_PICK = range(6)

MAX_REPOS = 10
MAX_FILES = 20
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_LIST_REPOS = 100
ENV_NAME_ALIASES = {"env", "env.txt", ".env.txt", "dotenv", ".env.example"}
ENV_REF_RE = re.compile(r"^\.?env(\d+)$", re.IGNORECASE)
ENV_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")

GITHUB_TOKEN_URL = (
    "https://github.com/settings/tokens/new?scopes=repo&description=RepoForge%20Telegram%20Bot"
)


def is_owner(user_id: int | None) -> bool:
    return user_id == OWNER_ID


def is_authorized(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id in all_allowed_ids()


async def deny(update: Update) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    text = (
        "You are not authorized to use this bot.\n\n"
        f"Your Telegram ID: <code>{uid}</code>\n"
        "Send this number to the owner. They can allow you with:\n"
        f"<code>/allow {uid}</code>"
    )
    if update.callback_query:
        await update.callback_query.answer("Not authorized. Check the message for your ID.", show_alert=True)
        if update.callback_query.message:
            await update.callback_query.message.reply_text(text, parse_mode=ParseMode.HTML)
        return
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


def github_token_for(user_id: int) -> str | None:
    return get_github_token(user_id)


def client_for(user_id: int) -> GitHubClient:
    token = github_token_for(user_id)
    if not token:
        raise GitHubError(
            "No GitHub token found. Send /start and paste your GitHub token first."
        )
    return GitHubClient(token)


def sanitize_repo_name(raw: str) -> str:
    name = raw.strip().replace(" ", "-")
    name = re.sub(r"[^A-Za-z0-9._-]", "", name)
    name = name.strip("-.")
    if name.lower().endswith(".git"):
        name = name[:-4].rstrip("-.")
    return name[:90]


def normalize_filename(name: str) -> str:
    base = Path(name.replace("\\", "/")).name.strip()
    if not base:
        return "file"
    lower = base.lower()
    if lower in ENV_NAME_ALIASES or lower in {"env", "environment"}:
        return ".env"
    return base[:200]


def upload_dir(user_id: int) -> Path:
    path = UPLOAD_ROOT / str(user_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def clear_uploads(user_id: int) -> None:
    path = UPLOAD_ROOT / str(user_id)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def reset_session(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.clear()


def repo_names(base: str, quantity: int) -> list[str]:
    return [f"{base}{i}" for i in range(1, quantity + 1)]


def html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def format_file_list(names: list[str]) -> str:
    if not names:
        return "<i>none yet</i>"
    return "\n".join(f"• <code>{html_escape(n)}</code>" for n in names)


def visibility_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬛  Public", callback_data="vis:public"),
                InlineKeyboardButton("⬛  Private", callback_data="vis:private"),
            ]
        ]
    )


def quantity_keyboard() -> InlineKeyboardMarkup:
    row1 = [InlineKeyboardButton(f"⬛ {i}", callback_data=f"qty:{i}") for i in range(1, 6)]
    row2 = [InlineKeyboardButton(f"⬛ {i}", callback_data=f"qty:{i}") for i in range(6, 11)]
    return InlineKeyboardMarkup([row1, row2])


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬛  Create", callback_data="menu:create"),
                InlineKeyboardButton("⬛  Edit", callback_data="menu:edit"),
            ],
            [
                InlineKeyboardButton("⬛  Repos", callback_data="menu:repos"),
                InlineKeyboardButton("⬛  Who", callback_data="menu:who"),
            ],
        ]
    )


def edit_repo_keyboard(repos: list[dict], page: int = 0, per_page: int = 8) -> InlineKeyboardMarkup:
    start = page * per_page
    chunk = repos[start : start + per_page]
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for repo in chunk:
        idx = int(repo.get("index") or 0)
        name = str(repo.get("name") or f"repo{idx}")
        lock = "🔒" if repo.get("private") else "🌐"
        label = f"⬛ {lock} {name}"[:34]
        row.append(InlineKeyboardButton(label, callback_data=f"editrepo:{idx}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬛  Prev", callback_data=f"editpage:{page - 1}"))
    if start + per_page < len(repos):
        nav.append(InlineKeyboardButton("⬛  Next", callback_data=f"editpage:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬛  Cancel", callback_data="editrepo:cancel")])
    return InlineKeyboardMarkup(rows)


def menu_text(owner: str | None = None) -> str:
    who = f"⬛  GitHub · <b>@{html_escape(owner)}</b>\n\n" if owner else "⬛\n\n"
    return (
        f"{who}"
        "Tap a black button, or type a command.\n\n"
        "/create — new repos\n"
        "/edit — change files in an existing repo\n"
        "/getrepo — every repo on the account\n"
        "/up .env1 value — patch BOT_TOKEN\n"
        "/token · /who · /allow · /deny · /help"
    )


TOKEN_PROMPT = (
    "⬛  GitHub token\n\n"
    "Classic PAT · <code>repo</code> scope · full access on <b>your</b> account.\n"
    f'<a href="{GITHUB_TOKEN_URL}">make one here</a> · tick <code>repo</code> · paste it.\n\n'
    "Not your password. I'll delete the message if I can."
)

FILES_PROMPT = (
    "⬛  Upload your files\n\n"
    "They land in every new repo:\n"
    "• <code>bot.py</code>\n"
    "• <code>requirements.txt</code>\n"
    "• <code>.env</code>\n\n"
    "📎 Paperclip → File\n"
    "/done when the set is complete."
)

EDIT_FILES_PROMPT = (
    "⬛  Drop replacement files\n\n"
    "Same names overwrite on GitHub. New names get added.\n"
    "📎 Paperclip → File · then /done"
)

HELP_TEXT = (
    "⬛  RepoForge\n\n"
    "/create — stamp out Devbot1…Devbot10\n"
    "/edit — push files into a repo that already exists\n"
    "/getrepo — list the account\n"
    "/up .env1 VALUE — BOT_TOKEN on repo 1\n"
    "/up .env2 KEY val — any key\n\n"
    "Friends (owner):\n"
    "<code>/allow ID</code> · <code>/deny ID</code> · /who\n"
    "Each person uses their own GitHub token."
)


TG_LIMIT = 3800


def _chunks(text: str, limit: int = TG_LIMIT) -> list[str]:
    parts: list[str] = []
    while text:
        if len(text) <= limit:
            parts.append(text)
            break
        cut = text.rfind("\n\n", 0, limit)
        if cut < 80:
            cut = text.rfind("\n", 0, limit)
        if cut < 80:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    return parts


async def send_html_parts(message, text: str, *, edit_first: bool = False) -> None:
    parts = _chunks(text)
    for i, part in enumerate(parts):
        if i == 0 and edit_first:
            try:
                await message.edit_text(
                    part,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                continue
            except Exception:
                pass
        await message.reply_text(
            part,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


def start_health_server() -> None:
    """Render web services must bind $PORT. UptimeRobot pings this."""
    raw = os.environ.get("PORT")
    if not raw:
        return
    try:
        port = int(raw)
    except ValueError:
        return

    class Ping(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")

        def do_HEAD(self) -> None:  # noqa: N802
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    httpd = HTTPServer(("0.0.0.0", port), Ping)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    logger.info("Health check listening on 0.0.0.0:%s", port)


async def _reply(update: Update, text: str, **kwargs):
    if update.callback_query and update.callback_query.message:
        return await update.callback_query.message.reply_text(text, **kwargs)
    if update.message:
        return await update.message.reply_text(text, **kwargs)
    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not is_authorized(user.id if user else None):
        await deny(update)
        return ConversationHandler.END

    reset_session(context)
    if user:
        clear_uploads(user.id)

    token = github_token_for(user.id) if user else None
    if token:
        owner = get_github_owner(user.id) if user else None
        if not owner:
            try:
                owner = await GitHubClient(token).get_owner()
                if user:
                    set_github_token(user.id, token, owner)
            except GitHubError:
                owner = None
        await _reply(
            update,
            menu_text(owner),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=main_keyboard(),
        )
        return ConversationHandler.END

    await _reply(
        update,
        TOKEN_PROMPT,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    return WAIT_TOKEN


async def token_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not is_authorized(user.id if user else None):
        await deny(update)
        return ConversationHandler.END
    await _reply(
        update,
        TOKEN_PROMPT,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    return WAIT_TOKEN


async def receive_token(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    raw = (update.message.text or "").strip()
    if raw.startswith("/"):
        await update.message.reply_text("Please send your GitHub token, or /cancel.")
        return WAIT_TOKEN
    if len(raw) < 20:
        await update.message.reply_text(
            "That is too short to be a GitHub token. Send a classic personal "
            "access token with the <code>repo</code> scope.",
            parse_mode=ParseMode.HTML,
        )
        return WAIT_TOKEN

    pending = await update.message.reply_text("Checking GitHub token…")
    try:
        client = GitHubClient(raw)
        owner = await client.get_owner()
    except GitHubError as exc:
        await pending.edit_text(
            f"GitHub rejected that token: {html_escape(str(exc))}\n\n"
            "Create a classic token with the <code>repo</code> scope and send it again.",
            parse_mode=ParseMode.HTML,
        )
        return WAIT_TOKEN

    set_github_token(user.id, raw, owner)
    try:
        await update.message.delete()
    except Exception:
        pass

    go_create = bool(context.user_data.pop("pending_create", False))
    go_edit = bool(context.user_data.pop("pending_edit", False))
    await pending.edit_text(
        f"⬛  GitHub · <b>@{html_escape(owner)}</b>\nToken tucked away.",
        parse_mode=ParseMode.HTML,
    )
    if go_create:
        return await _ask_files(update, context)
    if go_edit:
        return await _show_edit_picker(update, context)

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=menu_text(owner),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=main_keyboard(),
    )
    return ConversationHandler.END


async def create_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if update.callback_query:
        await update.callback_query.answer()
    if not is_authorized(user.id if user else None):
        await deny(update)
        return ConversationHandler.END

    if not github_token_for(user.id):
        context.user_data["pending_create"] = True
        await _reply(
            update,
            "⬛  GitHub first.\n\n" + TOKEN_PROMPT,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return WAIT_TOKEN

    reset_session(context)
    clear_uploads(user.id)
    return await _ask_files(update, context)


async def edit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if update.callback_query:
        await update.callback_query.answer()
    if not is_authorized(user.id if user else None):
        await deny(update)
        return ConversationHandler.END
    if not github_token_for(user.id):
        context.user_data["pending_edit"] = True
        await _reply(
            update,
            "⬛  GitHub first.\n\n" + TOKEN_PROMPT,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return WAIT_TOKEN
    reset_session(context)
    if user:
        clear_uploads(user.id)
    return await _show_edit_picker(update, context)


async def _load_edit_repos(user_id: int) -> list[dict]:
    repos = get_last_batch(user_id)
    gh = client_for(user_id)
    owner = await gh.get_owner()
    fetched = await gh.list_repos(max_repos=MAX_LIST_REPOS)
    numbered = []
    for i, repo in enumerate(fetched, start=1):
        numbered.append(
            {
                "index": i,
                "name": repo["name"],
                "owner": repo.get("owner") or owner,
                "url": repo.get("url") or "",
                "private": repo.get("private"),
            }
        )
    if numbered:
        set_numbered_repos(user_id, numbered)
        return numbered
    return repos


async def _show_edit_picker(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0) -> int:
    user = update.effective_user
    try:
        repos = await _load_edit_repos(user.id)
    except GitHubError as exc:
        await _reply(update, f"⬛  GitHub: {html_escape(str(exc))}", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    if not repos:
        await _reply(update, "⬛  No repos yet. /create first.", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    context.user_data["edit_page"] = page
    await _reply(
        update,
        "⬛  Pick a repo to edit\nExisting files with the same name get overwritten.",
        parse_mode=ParseMode.HTML,
        reply_markup=edit_repo_keyboard(repos, page=page),
    )
    return WAIT_EDIT_PICK


async def receive_edit_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user = update.effective_user
    if data == "editrepo:cancel":
        await query.message.reply_text("⬛  stopped", reply_markup=main_keyboard())
        return ConversationHandler.END
    if data.startswith("editpage:"):
        try:
            page = int(data.split(":", 1)[1])
        except ValueError:
            page = 0
        repos = get_last_batch(user.id)
        await query.message.edit_reply_markup(reply_markup=edit_repo_keyboard(repos, page=page))
        return WAIT_EDIT_PICK
    try:
        idx = int(data.split(":", 1)[1])
    except (IndexError, ValueError):
        return WAIT_EDIT_PICK
    repo = get_repo_by_index(user.id, idx)
    if not repo:
        await query.message.reply_text("⬛  that repo is gone. /edit again.")
        return WAIT_EDIT_PICK
    context.user_data["edit_repo"] = repo
    context.user_data["files"] = {}
    if user:
        clear_uploads(user.id)
    lock = "🔒" if repo.get("private") else "🌐"
    await query.message.reply_text(
        f"⬛  Editing {lock} <code>{html_escape(str(repo.get('name')))}</code>\n\n"
        + EDIT_FILES_PROMPT,
        parse_mode=ParseMode.HTML,
    )
    return WAIT_FILES


async def _push_edits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    repo = context.user_data.get("edit_repo") or {}
    files_map: dict = context.user_data.get("files") or {}
    name = str(repo.get("name") or "")
    url = str(repo.get("url") or "")
    file_bytes: dict[str, bytes] = {}
    for fname, path in files_map.items():
        try:
            file_bytes[fname] = Path(path).read_bytes()
        except Exception:
            await update.message.reply_text(
                f"Could not read <code>{html_escape(fname)}</code>.",
                parse_mode=ParseMode.HTML,
            )
            return WAIT_FILES
    status = await update.message.reply_text(
        f"⬛  pushing {len(file_bytes)} file(s) → <code>{html_escape(name)}</code>…",
        parse_mode=ParseMode.HTML,
    )
    try:
        gh = client_for(user.id)
        notes = await gh.update_files(name, file_bytes)
    except GitHubError as exc:
        await status.edit_text(
            f"⬛  GitHub: {html_escape(str(exc))}",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END
    lines = ["⬛  Done"]
    if url:
        lines.append(
            f'<a href="{html_escape(url)}">{html_escape(name)}</a>'
        )
    else:
        lines.append(f"<code>{html_escape(name)}</code>")
    for n in notes:
        kind, _, fname = n.partition(" ")
        mark = {"added": "✨", "updated": "⚡", "unchanged": "·"}.get(kind, "•")
        lines.append(f"{mark} <code>{html_escape(fname)}</code> · {html_escape(kind)}")
    lines.append("\n.env patch anytime: <code>/up .envN VALUE</code>")
    await status.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=main_keyboard(),
    )
    clear_uploads(user.id)
    reset_session(context)
    return ConversationHandler.END


async def _ask_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if user:
        clear_uploads(user.id)
    context.user_data["files"] = {}
    await _reply(update, FILES_PROMPT, parse_mode=ParseMode.HTML)
    return WAIT_FILES


async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    doc = update.message.document
    if not doc:
        await update.message.reply_text(
            "Please send a file as a document (paperclip → File), not as a photo."
        )
        return WAIT_FILES

    files: dict = context.user_data.setdefault("files", {})
    filename = normalize_filename(doc.file_name or f"file_{doc.file_id[-8:]}")
    if len(files) >= MAX_FILES and filename not in files:
        await update.message.reply_text(
            f"Maximum {MAX_FILES} files. Send /done or /cancel."
        )
        return WAIT_FILES

    if doc.file_size and doc.file_size > MAX_FILE_BYTES:
        await update.message.reply_text(
            f"<code>{html_escape(doc.file_name or 'file')}</code> is too large "
            f"(max {MAX_FILE_BYTES // (1024 * 1024)} MB).",
            parse_mode=ParseMode.HTML,
        )
        return WAIT_FILES

    dest = upload_dir(user.id) / filename
    try:
        tg_file = await doc.get_file()
        await tg_file.download_to_drive(custom_path=str(dest))
    except Exception:
        logger.exception("Failed to download file")
        await update.message.reply_text("Could not download that file. Please try again.")
        return WAIT_FILES

    files[filename] = dest
    names = list(files.keys())
    note = ""
    original = (doc.file_name or "").lower()
    if filename == ".env" and original not in {".env"}:
        note = (
            f"\n\nSaved <code>{html_escape(doc.file_name or '')}</code> as "
            f"<code>.env</code> so /up can update it later."
        )
    await update.message.reply_text(
        f"Saved <code>{html_escape(filename)}</code> "
        f"({len(names)} file{'s' if len(names) != 1 else ''}).\n\n"
        f"<b>Files so far</b>\n{format_file_list(names)}"
        f"{note}\n\n"
        "Send more files, or /done when you are finished.",
        parse_mode=ParseMode.HTML,
    )
    return WAIT_FILES


async def receive_non_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Please upload your files as documents (for example bot.py, requirements.txt, .env).\n"
        "Send /done when you are finished, or /cancel to stop."
    )
    return WAIT_FILES


async def done_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not is_authorized(user.id if user else None):
        await deny(update)
        return ConversationHandler.END

    files_map: dict = context.user_data.get("files") or {}
    if not files_map:
        prompt = EDIT_FILES_PROMPT if context.user_data.get("edit_repo") else FILES_PROMPT
        await update.message.reply_text(
            "No files yet.\n\n" + prompt,
            parse_mode=ParseMode.HTML,
        )
        return WAIT_FILES

    if context.user_data.get("edit_repo"):
        return await _push_edits(update, context)

    await update.message.reply_text(
        f"⬛  {len(files_map)} file(s)\n{format_file_list(list(files_map))}\n\n"
        f"How many repos? 1–{MAX_REPOS}",
        parse_mode=ParseMode.HTML,
        reply_markup=quantity_keyboard(),
    )
    return WAIT_QTY


async def _apply_quantity(message, context: ContextTypes.DEFAULT_TYPE, qty: int) -> int:
    context.user_data["quantity"] = qty
    await message.reply_text(
        f"⬛  x{qty}\nPublic or private?",
        parse_mode=ParseMode.HTML,
        reply_markup=visibility_keyboard(),
    )
    return WAIT_VIS


async def receive_qty(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.message.reply_text(
            f"Please send a number between 1 and {MAX_REPOS}.",
            reply_markup=quantity_keyboard(),
        )
        return WAIT_QTY
    qty = int(text)
    if qty < 1 or qty > MAX_REPOS:
        await update.message.reply_text(
            f"Please send a number between 1 and {MAX_REPOS}.",
            reply_markup=quantity_keyboard(),
        )
        return WAIT_QTY
    return await _apply_quantity(update.message, context, qty)


async def receive_qty_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    try:
        qty = int(data.split(":", 1)[1])
    except (IndexError, ValueError):
        return WAIT_QTY
    if qty < 1 or qty > MAX_REPOS:
        return WAIT_QTY
    return await _apply_quantity(query.message, context, qty)


async def receive_visibility_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if data not in ("vis:public", "vis:private"):
        return WAIT_VIS
    return await _ask_name(query.message, context, data.endswith("private"))


async def receive_visibility_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip().lower()
    if text in {"public", "pub", "open"}:
        private = False
    elif text in {"private", "pvt", "priv", "hidden"}:
        private = True
    else:
        await update.message.reply_text(
            "Please tap a button or type <code>public</code> or <code>private</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=visibility_keyboard(),
        )
        return WAIT_VIS
    return await _ask_name(update.message, context, private)


async def _ask_name(message, context: ContextTypes.DEFAULT_TYPE, private: bool) -> int:
    context.user_data["private"] = private
    vis = "private" if private else "public"
    await message.reply_text(
        f"All repositories will be <b>{vis}</b>.\n\n"
        "Send the repository <b>name</b>.\n\n"
        "Example: <code>Devbot</code>\n"
        "If quantity is 3 I will create:\n"
        "<code>Devbot1</code>, <code>Devbot2</code>, <code>Devbot3</code>",
        parse_mode=ParseMode.HTML,
    )
    return WAIT_NAME


async def receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = (update.message.text or "").strip()
    name = sanitize_repo_name(raw)
    if not name:
        await update.message.reply_text(
            "That name is not valid. Send something like <code>Devbot</code>.",
            parse_mode=ParseMode.HTML,
        )
        return WAIT_NAME
    context.user_data["base_name"] = name
    qty = int(context.user_data.get("quantity") or 1)
    names = repo_names(name, qty)
    listed = "\n".join(
        f"{i}. <code>{html_escape(n)}</code>" for i, n in enumerate(names, start=1)
    )
    await update.message.reply_text(
        f"⬛  <code>{html_escape(name)}</code>\n{listed}\n\nspinning up…",
        parse_mode=ParseMode.HTML,
    )
    return await create_all_repos(update, context)


async def create_all_repos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    files_map: dict = context.user_data.get("files") or {}
    base = context.user_data.get("base_name")
    qty = int(context.user_data.get("quantity") or 0)
    private = bool(context.user_data.get("private"))

    if not base or not qty or not files_map:
        await update.message.reply_text("Setup is incomplete. Send /create to start again.")
        return ConversationHandler.END

    file_bytes: dict[str, bytes] = {}
    for fname, path in files_map.items():
        try:
            file_bytes[fname] = Path(path).read_bytes()
        except Exception:
            await update.message.reply_text(
                f"Could not read <code>{html_escape(fname)}</code>. Send /create and upload again.",
                parse_mode=ParseMode.HTML,
            )
            return ConversationHandler.END

    names = repo_names(base, qty)
    vis = "private" if private else "public"
    file_names = list(file_bytes.keys())
    status = await update.message.reply_text(
        f"Creating <b>{qty}</b> {vis} repositories and pushing "
        f"<b>{len(file_bytes)}</b> file(s)…",
        parse_mode=ParseMode.HTML,
    )

    try:
        gh = client_for(user.id)
        owner = await gh.get_owner()
    except GitHubError as exc:
        await status.edit_text(
            f"GitHub error: {html_escape(str(exc))}\nSend /start and paste your token again.",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    taken = []
    try:
        for n in names:
            if await gh.repo_exists(n):
                taken.append(n)
    except GitHubError as exc:
        await status.edit_text(
            f"Could not check existing repositories: {html_escape(str(exc))}",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    if taken:
        listed = ", ".join(f"<code>{html_escape(n)}</code>" for n in taken)
        await status.edit_text(
            f"These names already exist on @{html_escape(owner)}:\n{listed}\n\n"
            "Send /create again with a different name.",
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    created: list[dict] = []
    failures: list[str] = []
    log_lines = [
        f"Account: @{html_escape(owner)}",
        f"Visibility: {vis}",
        "",
    ]

    for index, name in enumerate(names, start=1):
        log_lines.append(f"{index}/{qty} <code>{html_escape(name)}</code>…")
        try:
            await status.edit_text("\n".join(log_lines), parse_mode=ParseMode.HTML)
        except Exception:
            pass
        try:
            info = await gh.create_repo(name, private=private)
            await asyncio.sleep(0.6)
            await gh.push_files(name, file_bytes)
            url = info.get("html_url") or f"https://github.com/{owner}/{name}"
            created.append(
                {
                    "index": index,
                    "name": name,
                    "owner": owner,
                    "url": url,
                    "private": private,
                    "files": file_names,
                }
            )
            log_lines[-1] = (
                f"{index}/{qty} ✅ <a href=\"{html_escape(url)}\">{html_escape(name)}</a>"
            )
        except GitHubError as exc:
            logger.exception("Failed on %s", name)
            failures.append(f"{name}: {exc}")
            log_lines[-1] = (
                f"{index}/{qty} ❌ <code>{html_escape(name)}</code> — {html_escape(str(exc))}"
            )
        except Exception:
            logger.exception("Unexpected failure on %s", name)
            failures.append(f"{name}: unexpected error")
            log_lines[-1] = (
                f"{index}/{qty} ❌ <code>{html_escape(name)}</code> — unexpected error"
            )
        try:
            await status.edit_text("\n".join(log_lines), parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await asyncio.sleep(0.8)

    if created:
        save_batch(user.id, created)

    summary = ["\n<b>Created repositories</b>"]
    if created:
        for repo in created:
            vis_l = "private" if repo.get("private") else "public"
            summary.append("")
            summary.append(
                f"{repo['index']}. <a href=\"{html_escape(repo['url'])}\">"
                f"{html_escape(repo['name'])}</a>  [{vis_l}]"
            )
            summary.append("   Files:")
            for fn in repo.get("files") or file_names:
                summary.append(f"   • <code>{html_escape(fn)}</code>")
            summary.append(f"   Update: <code>/up .env{repo['index']} YOUR_BOT_TOKEN</code>")
        summary.append("")
        summary.append("List every GitHub repo: /getrepo")
    if failures:
        summary.append("")
        summary.append("<b>Failed</b>")
        for item in failures:
            summary.append(f"• {html_escape(item)}")

    final = "\n".join(log_lines + summary)
    await send_html_parts(status, final, edit_first=True)

    clear_uploads(user.id)
    reset_session(context)
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if user:
        clear_uploads(user.id)
    reset_session(context)
    if update.message:
        await update.message.reply_text("Cancelled. Send /start or /create when you want to continue.")
    return ConversationHandler.END


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id if update.effective_user else None):
        await deny(update)
        return
    msg = update.effective_message
    await msg.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_keyboard())


async def getrepo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_authorized(user.id if user else None):
        await deny(update)
        return
    msg = update.effective_message
    if not github_token_for(user.id):
        await msg.reply_text("⬛  GitHub first. /start")
        return

    pending = await msg.reply_text("⬛  loading repos…")
    try:
        gh = client_for(user.id)
        owner = await gh.get_owner()
        repos = await gh.list_repos(max_repos=MAX_LIST_REPOS)
    except GitHubError as exc:
        await pending.edit_text(
            f"GitHub error: {html_escape(str(exc))}",
            parse_mode=ParseMode.HTML,
        )
        return

    numbered = []
    for i, repo in enumerate(repos, start=1):
        numbered.append(
            {
                "index": i,
                "name": repo["name"],
                "owner": repo.get("owner") or owner,
                "url": repo.get("url") or "",
                "private": repo.get("private"),
            }
        )
    set_numbered_repos(user.id, numbered)

    if not numbered:
        await pending.edit_text(
            f"No repositories found on @{html_escape(owner)}.",
            parse_mode=ParseMode.HTML,
        )
        return

    header = (
        f"⬛  Repos on @{html_escape(owner)}  ({len(numbered)})\n"
        "Files: /edit · token: <code>/up .envN VALUE</code>\n"
    )
    chunks: list[str] = []
    buf = header
    for repo in numbered:
        vis = "🔒" if repo.get("private") else "🌐"
        url = html_escape(str(repo.get("url") or ""))
        name = html_escape(str(repo.get("name") or ""))
        line = f"\n{repo['index']}. {vis} <a href=\"{url}\">{name}</a>  ·  .env{repo['index']}"
        if len(buf) + len(line) > 3500:
            chunks.append(buf)
            buf = line.lstrip("\n")
        else:
            buf += line
    chunks.append(buf)

    await pending.edit_text(
        chunks[0],
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    for extra in chunks[1:]:
        await pending.reply_text(
            extra,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


async def up_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_authorized(user.id if user else None):
        await deny(update)
        return

    usage = (
        "<b>Usage</b>\n"
        "<code>/up .env1 YOUR_BOT_TOKEN</code>\n"
        "Updates <code>BOT_TOKEN</code> in repository 1's .env file.\n\n"
        "<code>/up .env2 KEY value</code>\n"
        "Updates any key in repository 2's .env file.\n\n"
        "Numbers come from the last /create batch, or from /getrepo."
    )

    text = (update.message.text or "").strip()
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text(usage, parse_mode=ParseMode.HTML)
        return

    ref = parts[1]
    match = ENV_REF_RE.match(ref)
    if not match:
        await update.message.reply_text(
            "The second word must look like <code>.env1</code>, <code>.env2</code>, …\n\n"
            + usage,
            parse_mode=ParseMode.HTML,
        )
        return

    index = int(match.group(1))
    if index < 1:
        await update.message.reply_text("Repository number must be 1 or higher.")
        return

    rest_parts = parts[2:]
    if len(rest_parts) >= 2 and ENV_KEY_RE.match(rest_parts[0]):
        key = rest_parts[0]
        split3 = text.split(maxsplit=3)
        value = split3[3] if len(split3) > 3 else ""
    else:
        key = "BOT_TOKEN"
        split2 = text.split(maxsplit=2)
        value = split2[2] if len(split2) > 2 else ""

    value = value.strip()
    if not value:
        await update.message.reply_text("Missing value.\n\n" + usage, parse_mode=ParseMode.HTML)
        return

    if not github_token_for(user.id):
        await update.message.reply_text("Connect GitHub first with /start.")
        return

    repo = get_repo_by_index(user.id, index)
    if not repo:
        await update.message.reply_text(
            f"No repository mapped to <code>.env{index}</code>.\n"
            "Create repos with /create, or send /getrepo to number all GitHub repos.",
            parse_mode=ParseMode.HTML,
        )
        return

    name = repo["name"]
    url = repo.get("url") or ""
    pending = await update.message.reply_text(
        f"Updating <code>{html_escape(key)}</code> in "
        f"<code>{html_escape(name)}</code>/.env …",
        parse_mode=ParseMode.HTML,
    )
    try:
        gh = client_for(user.id)
        result = await gh.upsert_env_key(name, key, value)
    except GitHubError as exc:
        await pending.edit_text(
            f"GitHub error while updating <code>{html_escape(name)}</code>:\n"
            f"{html_escape(str(exc))}",
            parse_mode=ParseMode.HTML,
        )
        return

    verb = {
        "created": "Created .env and set",
        "updated": "Updated",
        "unchanged": "Already set — no commit needed for",
    }.get(result, "Saved")
    link = (
        f"<a href=\"{html_escape(url)}\">{html_escape(name)}</a>"
        if url
        else f"<code>{html_escape(name)}</code>"
    )
    await pending.edit_text(
        f"{verb} <code>{html_escape(key)}</code> in {link} "
        f"(<code>.env{index}</code>).",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


def _parse_ids_from_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> list[int]:
    ids: list[int] = []
    for arg in context.args or []:
        bit = re.sub(r"[^\d]", "", arg)
        if bit.isdigit() and len(bit) >= 5:
            ids.append(int(bit))
    msg = update.effective_message
    text = (msg.text or msg.caption or "") if msg else ""
    for bit in re.findall(r"\d{5,}", text):
        ids.append(int(bit))
    if msg and msg.reply_to_message and msg.reply_to_message.from_user:
        ids.append(msg.reply_to_message.from_user.id)
    if msg and msg.forward_from:
        ids.append(msg.forward_from.id)
    seen: set[int] = set()
    out: list[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


async def allow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    user = update.effective_user
    try:
        if not is_owner(user.id if user else None):
            await msg.reply_text("⬛  Only the owner can add friends.")
            return
        ids = _parse_ids_from_update(update, context)
        if not ids:
            await msg.reply_text(
                "⬛  Send it like this:\n"
                "<code>/allow 123456789</code>\n"
                "Or reply to their message with /allow.",
                parse_mode=ParseMode.HTML,
            )
            return
        extra = extra_allowed_ids()
        added = []
        already = []
        for i in ids:
            if i == OWNER_ID or i in extra:
                already.append(i)
                continue
            extra.add(i)
            added.append(i)
        _save_extra_allowed(extra)
        lines = ["⬛  Access updated"]
        if added:
            lines.append("Allowed:")
            lines.extend(f"• <code>{i}</code>" for i in added)
        if already:
            lines.append("Already in:")
            lines.extend(f"• <code>{i}</code>" for i in already)
        lines.append("\nThey tap /start, then paste <b>their</b> GitHub token.")
        await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.exception("allow_cmd failed")
        await msg.reply_text(f"⬛  /allow failed: {html_escape(str(exc))}", parse_mode=ParseMode.HTML)


async def deny_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    msg = update.effective_message
    if not msg:
        return
    if not is_owner(user.id if user else None):
        await msg.reply_text("⬛  Only the owner can remove friends.")
        return
    ids = _parse_ids_from_update(update, context)
    if not ids:
        await msg.reply_text(
            "⬛  <code>/deny 123456789</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    extra = extra_allowed_ids()
    removed = []
    skipped = []
    for i in ids:
        if i == OWNER_ID:
            skipped.append(i)
            continue
        if i in extra:
            extra.discard(i)
            removed.append(i)
        else:
            skipped.append(i)
    _save_extra_allowed(extra)
    lines = []
    if removed:
        lines.append("Removed:")
        lines.extend(f"• <code>{i}</code>" for i in removed)
    if skipped:
        lines.append("Not in the list (or owner):")
        lines.extend(f"• <code>{i}</code>" for i in skipped)
    await update.message.reply_text("\n".join(lines) or "Nothing changed.", parse_mode=ParseMode.HTML)


async def who_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not is_authorized(user.id if user else None):
        await deny(update)
        return
    extra = sorted(extra_allowed_ids())
    lines = [
        "⬛  Access",
        f"Owner: <code>{OWNER_ID}</code>",
    ]
    if extra:
        lines.append("Friends:")
        lines.extend(f"• <code>{i}</code>" for i in extra)
    else:
        lines.append("No friends yet. Owner: <code>/allow USER_ID</code>")
    msg = update.effective_message
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())


def build_app() -> Application:
    if not BOT_TOKEN:
        raise SystemExit(
            "BOT_TOKEN is missing. Copy .env.example to .env and add your Telegram bot token."
        )

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("create", create_cmd),
            CommandHandler("edit", edit_cmd),
            CommandHandler("token", token_cmd),
            CommandHandler("setgithub", token_cmd),
            CallbackQueryHandler(create_cmd, pattern=r"^menu:create$"),
            CallbackQueryHandler(edit_cmd, pattern=r"^menu:edit$"),
        ],
        states={
            WAIT_TOKEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_token),
            ],
            WAIT_FILES: [
                MessageHandler(filters.Document.ALL, receive_file),
                CommandHandler("done", done_cmd),
                MessageHandler(filters.PHOTO, receive_non_file),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_non_file),
            ],
            WAIT_QTY: [
                CallbackQueryHandler(receive_qty_cb, pattern=r"^qty:\d+$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_qty),
            ],
            WAIT_VIS: [
                CallbackQueryHandler(receive_visibility_cb, pattern=r"^vis:(public|private)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_visibility_text),
            ],
            WAIT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_name),
            ],
            WAIT_EDIT_PICK: [
                CallbackQueryHandler(receive_edit_pick_cb, pattern=r"^edit(repo|page):"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            CommandHandler("start", start),
            CommandHandler("create", create_cmd),
            CommandHandler("edit", edit_cmd),
            CommandHandler("token", token_cmd),
        ],
        allow_reentry=True,
        name="repoforge",
    )

    async def menu_extra(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        q = update.callback_query
        if q:
            await q.answer()
        data = (q.data if q else "") or ""
        if data == "menu:repos":
            await getrepo_cmd(update, context)
        elif data == "menu:who":
            await who_cmd(update, context)

    always = [
        ("allow", allow_cmd),
        ("deny", deny_cmd),
        ("who", who_cmd),
        ("help", help_cmd),
        ("getrepo", getrepo_cmd),
        ("up", up_cmd),
        ("cancel", cancel_cmd),
    ]
    for name, fn in always:
        app.add_handler(CommandHandler(name, fn), group=-1)
    app.add_handler(CallbackQueryHandler(menu_extra, pattern=r"^menu:(repos|who)$"))
    app.add_handler(conv)
    return app


def _ensure_event_loop() -> None:
    # Python 3.12+ / 3.14 (Render default) has no loop on MainThread.
    # python-telegram-bot 21 calls asyncio.get_event_loop() in run_polling.
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)


def main() -> None:
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    start_health_server()
    _ensure_event_loop()
    app = build_app()
    logger.info("RepoForge bot starting (polling)… owner=%s extra=%s", OWNER_ID, extra_allowed_ids())
    app.run_polling(drop_pending_updates=True, stop_signals=None)


if __name__ == "__main__":
    main()
