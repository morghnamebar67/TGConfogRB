"""
╔══════════════════════════════════════════════════════════════╗
║        Telegram → Rubika VPN Forwarder Bot                  ║
║        Single-file • Production-grade • Feature-complete    ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import asyncio
import json
import logging
import time
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import aiofiles
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    DocumentAttributeVideo, DocumentAttributeAudio,
    DocumentAttributeFilename,
)

# ──────────────────────────────────────────────────────────────
# ENV CONFIG
# ──────────────────────────────────────────────────────────────
API_ID         = int(os.environ["API_ID"])
API_HASH       = os.environ["API_HASH"]
STRING_SESSION = os.environ["STRING_SESSION"]
RUBIKA_BOT_TOKEN = os.environ["RUBIKA_BOT_TOKEN"]
DATA_REPO_PAT  = os.environ["DATA_REPO_PAT"]
DATA_REPO_URL  = os.environ["DATA_REPO_URL"]
ADMIN_CHAT_ID  = os.environ.get("ADMIN_CHAT_ID", "").strip()

RUN_DURATION   = 20_400          # ~5 h 40 min
POLL_INTERVAL  = 60              # subscriber poll interval (s)
CHANNELS_FILE  = "channels.json"
DATA_REPO_DIR  = Path("data_repo")
SUBS_FILE      = DATA_REPO_DIR / "subscribers.json"
STATE_FILE     = DATA_REPO_DIR / "state.json"

RUBIKA_BASE    = f"https://botapi.rubika.ir/v3/{RUBIKA_BOT_TOKEN}"

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("forwarder")

# ──────────────────────────────────────────────────────────────
# FILE SIZE LIMITS (bytes)
# ──────────────────────────────────────────────────────────────
SIZE_LIMITS = {
    "Image": 10 * 1024 * 1024,
    "Video": 50 * 1024 * 1024,
    "Voice": 20 * 1024 * 1024,
    "Music": 50 * 1024 * 1024,
    "File":  50 * 1024 * 1024,
    "Gif":   50 * 1024 * 1024,
}

# ──────────────────────────────────────────────────────────────
# VPN / PROXY URI PATTERNS (lines wrapped in mono+quote)
# ──────────────────────────────────────────────────────────────
VPN_SCHEMES = re.compile(
    r"^(vmess://|vless://|ss://|trojan://|tuic://|hysteria2?://|hy2://|"
    r"wireguard://|warp://|socks5?://|http://\d|https://\d|"
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
    re.IGNORECASE,
)

# ──────────────────────────────────────────────────────────────
# GLOBAL STATE
# ──────────────────────────────────────────────────────────────
subscribers:   set  = set()
pending_edits: dict = {}      # rubika_msg_id → {chat_id, msg_id, text, schedule}
update_offset: str  = None    # Rubika getUpdates offset


# ══════════════════════════════════════════════════════════════
# GIT / DATA REPO
# ══════════════════════════════════════════════════════════════

def _authenticated_url(url: str) -> str:
    """
    Inject PAT into a GitHub HTTPS URL so git never needs to prompt.
    https://github.com/owner/repo  →  https://x-token:PAT@github.com/owner/repo
    Works in GitHub Actions, Docker, any environment.
    """
    pat = DATA_REPO_PAT
    # Handle both https:// and git+https:// forms
    if "://" in url:
        scheme, rest = url.split("://", 1)
        # Strip any existing credentials (safety)
        if "@" in rest:
            rest = rest.split("@", 1)[1]
        return f"{scheme}://x-token:{pat}@{rest}"
    return url


def git_clone():
    auth_url = _authenticated_url(DATA_REPO_URL)
    log.info("Cloning data repo …")
    result = subprocess.run(
        ["git", "clone", auth_url, str(DATA_REPO_DIR)],
        capture_output=True,
    )
    if result.returncode != 0:
        # Redact PAT from error output before logging
        err = result.stderr.decode(errors="replace").replace(DATA_REPO_PAT, "***")
        raise RuntimeError(f"git clone failed (exit {result.returncode}): {err}")
    log.info("Data repo cloned ✓")


def git_push(message: str = "update"):
    auth_url = _authenticated_url(DATA_REPO_URL)
    try:
        # Embed PAT into remote URL so push authenticates without prompting
        subprocess.run(
            ["git", "-C", str(DATA_REPO_DIR), "remote", "set-url", "origin", auth_url],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(DATA_REPO_DIR), "config", "user.email", "bot@rubika"],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(DATA_REPO_DIR), "config", "user.name", "RubikaBot"],
            check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(DATA_REPO_DIR), "add", "-A"],
            check=True, capture_output=True,
        )
        result = subprocess.run(
            ["git", "-C", str(DATA_REPO_DIR), "commit", "-m", message],
            capture_output=True,
        )
        if result.returncode not in (0, 1):
            log.warning("git commit returned %s", result.returncode)
            return
        push_result = subprocess.run(
            ["git", "-C", str(DATA_REPO_DIR), "push"],
            capture_output=True,
        )
        if push_result.returncode != 0:
            err = push_result.stderr.decode(errors="replace").replace(DATA_REPO_PAT, "***")
            log.error("git push failed: %s", err)
        else:
            log.info("git push ✓ — %s", message)
    except subprocess.CalledProcessError as e:
        err = e.stderr.decode(errors="replace").replace(DATA_REPO_PAT, "***")
        log.error("git error: %s", err)


# ══════════════════════════════════════════════════════════════
# SUBSCRIBER PERSISTENCE
# ══════════════════════════════════════════════════════════════

def load_subscribers():
    global subscribers
    if SUBS_FILE.exists():
        try:
            data = json.loads(SUBS_FILE.read_text())
            subscribers = set(str(x) for x in data)
            log.info("Loaded %d subscribers", len(subscribers))
        except Exception as e:
            log.warning("Could not load subscribers: %s", e)
            subscribers = set()
    else:
        subscribers = set()


def save_subscribers():
    SUBS_FILE.write_text(json.dumps(sorted(subscribers), ensure_ascii=False, indent=2))


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_state(data: dict):
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))


# ══════════════════════════════════════════════════════════════
# RUBIKA API
# ══════════════════════════════════════════════════════════════

async def rubika_post(method: str, payload: dict, session: aiohttp.ClientSession,
                      retries: int = 3) -> Optional[dict]:
    url = f"{RUBIKA_BASE}/{method}"
    for attempt in range(1, retries + 1):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status in (429, 502, 503, 504):
                    wait = 2 ** attempt
                    log.warning("%s HTTP %s — retry in %ss", method, r.status, wait)
                    await asyncio.sleep(wait)
                    continue
                data = await r.json(content_type=None)
                status = (data.get("status") or "").upper()
                if status == "AUTH_ERROR":
                    log.critical("Rubika auth error: %s", data)
                    return None
                if status == "TOO_REQUESTS":
                    wait = 2 ** attempt
                    log.warning("%s rate-limited — retry in %ss", method, wait)
                    await asyncio.sleep(wait)
                    continue
                if status not in ("OK", ""):
                    err = data.get("status_det", "") or data.get("dev_message", "")
                    if "transient" in str(err).lower():
                        await asyncio.sleep(2 ** attempt)
                        continue
                    log.warning("%s API error: %s", method, data)
                    return data   # still return so callers can inspect
                return data
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            log.warning("%s connection error (attempt %d): %s", method, attempt, e)
            await asyncio.sleep(2 ** attempt)
    return None


async def rubika_send_message(chat_id: str, text: str,
                               session: aiohttp.ClientSession,
                               metadata: Optional[dict] = None) -> Optional[str]:
    payload = {"chat_id": chat_id, "text": text}
    if metadata:
        payload["metadata"] = metadata
    resp = await rubika_post("sendMessage", payload, session)
    if resp and "data" in resp:
        return resp["data"].get("message_id")
    return None


async def rubika_edit_message(chat_id: str, message_id: str, text: str,
                               session: aiohttp.ClientSession):
    await rubika_post("editMessageText",
                      {"chat_id": chat_id, "message_id": message_id, "text": text},
                      session)


async def rubika_upload_file(file_bytes: bytes, file_type: str,
                              session: aiohttp.ClientSession) -> Optional[str]:
    """Upload bytes → returns file_id or None."""
    # Step 1: request upload URL
    resp = await rubika_post("requestSendFile", {"type": file_type}, session)
    if not resp or "data" not in resp:
        log.error("requestSendFile failed")
        return None
    upload_url = resp["data"].get("upload_url")
    if not upload_url:
        log.error("No upload_url in response")
        return None

    # Step 2: POST as multipart/form-data with field name "file"
    # Rubika docs: file must be sent in the request body as multipart
    import json as _json
    try:
        form = aiohttp.FormData()
        form.add_field("file", file_bytes, filename="upload",
                       content_type="application/octet-stream")
        async with session.post(
            upload_url,
            data=form,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as r:
            raw = await r.read()

            # HTML response = nginx/server error (wrong method, bad URL, etc.)
            if raw.lstrip().startswith(b"<"):
                log.error("Upload got HTML error (HTTP %s) — wrong format or URL expired",
                          r.status)
                return None

            # Empty or null body → file accepted; extract token from URL path
            if not raw or raw.strip() in (b"", b"null"):
                file_id = upload_url.rstrip("/").split("/")[-1]
                log.info("Upload OK (empty body) — file_id: %s", file_id)
                return file_id

            # JSON response
            try:
                data = _json.loads(raw)
                file_id = (
                    data.get("file_id")
                    or (data.get("data") or {}).get("file_id")
                )
                if file_id:
                    log.info("Upload OK — file_id: %s", file_id)
                    return file_id
                log.error("Upload: no file_id in JSON response: %s", data)
            except _json.JSONDecodeError:
                token = raw.decode(errors="replace").strip()
                # Some Rubika servers return just the file_id token as plain text
                if token and len(token) < 200 and " " not in token and not token.startswith("<"):
                    log.info("Upload OK — plain-text file_id: %s", token)
                    return token
                log.error("Upload unreadable response (HTTP %s): %r", r.status, raw[:200])
    except Exception as e:
        log.error("File upload exception: %s", e)
    return None


async def rubika_send_file(chat_id: str, file_id: str, caption: str,
                            session: aiohttp.ClientSession) -> Optional[str]:
    payload = {"chat_id": chat_id, "file_id": file_id, "text": caption}
    resp = await rubika_post("sendFile", payload, session)
    if resp and "data" in resp:
        return resp["data"].get("message_id")
    return None


MAX_CHARS = 4800   # stay safely under 5000
MAX_LINES = 120    # stay safely under 128


def _split_message(text: str) -> list[str]:
    """Split a long message into chunks that fit Rubika limits."""
    lines = text.splitlines(keepends=True)
    chunks, current, clines = [], [], 0
    for line in lines:
        if (clines + 1 > MAX_LINES) or (sum(len(l) for l in current) + len(line) > MAX_CHARS):
            if current:
                chunks.append("".join(current))
            current, clines = [line], 1
        else:
            current.append(line)
            clines += 1
    if current:
        chunks.append("".join(current))
    return chunks or [text]


async def broadcast_text(text: str, session: aiohttp.ClientSession,
                         metadata: Optional[dict] = None) -> list:
    """
    Send text to all subscribers, splitting if needed.
    Metadata is only sent with the first chunk (offsets are valid there);
    continuation chunks are sent as plain text with a part indicator.
    Returns list of (chat_id, last_message_id).
    """
    chunks = _split_message(text)
    results = []
    for chat_id in list(subscribers):
        last_mid = None
        for i, chunk in enumerate(chunks):
            # Only attach metadata to the first chunk; subsequent chunks are plain
            chunk_meta = metadata if i == 0 else None
            if len(chunks) > 1:
                part_note = "\n\n📄 بخش " + str(i + 1) + " از " + str(len(chunks))
                chunk = chunk + part_note
            mid = await rubika_send_message(chat_id, chunk, session, metadata=chunk_meta)
            if mid:
                last_mid = mid
            await asyncio.sleep(0.12)
        if last_mid:
            results.append((chat_id, last_mid))
        await asyncio.sleep(0.05)
    return results


async def broadcast_file(file_id: str, caption: str,
                          session: aiohttp.ClientSession) -> list:
    results = []
    for chat_id in list(subscribers):
        mid = await rubika_send_file(chat_id, file_id, caption, session)
        if mid:
            results.append((chat_id, mid))
        await asyncio.sleep(0.05)
    return results


# ══════════════════════════════════════════════════════════════
# MESSAGE FORMATTING
# ══════════════════════════════════════════════════════════════

DIVIDER = "―――――――――――――――――――――――――"

# Map Telethon entity types → Rubika MetadataTypeEnum
# import lazily to avoid top-level issues
def _tg_entity_to_rubika_type(ent) -> Optional[str]:
    """Return Rubika metadata type string for a Telethon MessageEntity, or None to skip."""
    from telethon.tl.types import (
        MessageEntityBold, MessageEntityItalic,
        MessageEntityCode, MessageEntityPre,
        MessageEntityStrike, MessageEntityUnderline,
        MessageEntitySpoiler,
    )
    if isinstance(ent, MessageEntityBold):      return "Bold"
    if isinstance(ent, MessageEntityItalic):    return "Italic"
    if isinstance(ent, MessageEntityCode):      return "Mono"
    if isinstance(ent, MessageEntityPre):       return "Pre"
    if isinstance(ent, MessageEntityStrike):    return "Strike"
    if isinstance(ent, MessageEntityUnderline): return "Underline"
    if isinstance(ent, MessageEntitySpoiler):   return "Spoiler"
    return None


def _channel_display(entity) -> str:
    """Return best display name for a Telegram entity."""
    if entity is None:
        return "کانال"
    title    = getattr(entity, "title",    None)
    username = getattr(entity, "username", None)
    return title or (f"@{username}" if username else "کانال")


def _utf16_len(s: str) -> int:
    """Length in UTF-16 code units — Rubika metadata uses UTF-16 indexing."""
    return len(s.encode("utf-16-le")) // 2


def _utf16_to_char(text: str, utf16_offset: int) -> int:
    """Convert a UTF-16 offset into a Python str character index."""
    encoded = text.encode("utf-16-le")
    byte_pos = utf16_offset * 2
    if byte_pos >= len(encoded):
        return len(text)
    return len(encoded[:byte_pos].decode("utf-16-le"))


def _strip_raw_markdown(text: str) -> str:
    """
    Remove any residual raw markdown markers that leaked as plain text
    (happens when Telegram clients send MarkdownV1 text without entities).
    Strips ** __ ` ~~ but keeps the content between them.
    """
    # Bold: **text**
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text, flags=re.DOTALL)
    # Underline / italic: __text__
    text = re.sub(r'__(.+?)__', r'\1', text, flags=re.DOTALL)
    # Mono: `text`  (single or double backtick)
    text = re.sub(r'``(.+?)``', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'`(.+?)`',   r'\1', text, flags=re.DOTALL)
    # Strike: ~~text~~
    text = re.sub(r'~~(.+?)~~', r'\1', text, flags=re.DOTALL)
    # Orphan markers (no matching pair, just loose * ` chars)
    text = re.sub(r'(?<!\w)\*{1,2}(?!\w)', '', text)
    text = re.sub(r'(?<!\w)`(?!\w)',        '', text)
    return text


def _build_body(
    raw_text: str,
    tg_entities: list,          # Telethon MessageEntity objects (may be None)
    base_offset: int,           # UTF-16 offset where body starts in full message
) -> tuple[str, list[dict]]:
    """
    Build the message body:
    • Strips raw markdown fallback markers from plain text
    • Converts Telethon entities → Rubika metadata (Bold/Italic/Mono/…)
    • Detects VPN/IP lines → wraps them in Mono + Quote metadata blocks
    Returns (plain_body_text, rubika_meta_parts_list)
    """
    # 1. Clean residual raw markdown from the plain text
    clean = _strip_raw_markdown(raw_text)
    lines = clean.splitlines()

    # 2. Classify lines: vpn/ip → mono+quote block; others → plain
    #    We rebuild the body text from classified segments.
    segments: list[tuple[str, list[str]]] = []  # (text, [RubikaTypes])
    vpn_buf: list[str] = []

    def flush_vpn():
        if not vpn_buf:
            return
        segments.append(("\n".join(vpn_buf), ["Mono", "Quote"]))
        vpn_buf.clear()

    for line in lines:
        s = line.strip()
        if s and VPN_SCHEMES.match(s):
            # VPN/IP line — goes into the shared buffer regardless of what came before
            vpn_buf.append(s)
        else:
            flush_vpn()
            segments.append((s, []))

    flush_vpn()

    # Strip leading/trailing empty plain segments so that IP blocks at the
    # start or end of a message aren't preceded/followed by a blank line
    # that would visually separate the first IP from the rest of the block
    while segments and segments[0] == ("", []):
        segments.pop(0)
    while segments and segments[-1] == ("", []):
        segments.pop()

    # 3. Stitch segments → plain_body + structural (VPN) meta
    plain_parts: list[str] = []
    body_cursor = 0          # UTF-16 cursor relative to body start
    struct_meta: list[dict] = []

    for i, (seg, types) in enumerate(segments):
        if i > 0:
            plain_parts.append("\n")
            body_cursor += 1
        seg_u16 = _utf16_len(seg)
        for t in types:
            if seg_u16 > 0:
                struct_meta.append({
                    "type":       t,
                    "from_index": base_offset + body_cursor,
                    "length":     seg_u16,
                })
        plain_parts.append(seg)
        body_cursor += seg_u16

    plain_body = "".join(plain_parts)

    # 4. Map Telethon entities → Rubika metadata
    #    Telethon entity offsets are UTF-16 into the ORIGINAL raw_text.
    #    We apply them to the clean text (minimal change since we only stripped
    #    markdown markers which shift offsets slightly — but in practice
    #    Telegram sends entities OR markdown, rarely both, so this is safe).
    entity_meta: list[dict] = []
    for ent in (tg_entities or []):
        rtype = _tg_entity_to_rubika_type(ent)
        if not rtype:
            continue
        # ent.offset / ent.length are UTF-16 units into raw_text
        # Map to clean text: subtract chars removed before this offset
        raw_before  = raw_text.encode("utf-16-le")[: ent.offset * 2].decode("utf-16-le")
        clean_before = _strip_raw_markdown(raw_before)
        clean_start_u16 = _utf16_len(clean_before)

        raw_span    = raw_text.encode("utf-16-le")[ent.offset * 2 : (ent.offset + ent.length) * 2].decode("utf-16-le")
        clean_span  = _strip_raw_markdown(raw_span)
        clean_len_u16 = _utf16_len(clean_span)

        if clean_len_u16 > 0:
            entity_meta.append({
                "type":       rtype,
                "from_index": base_offset + clean_start_u16,
                "length":     clean_len_u16,
            })

    all_meta = struct_meta + entity_meta
    return plain_body, all_meta


def format_message(
    raw_text: str,
    channel_name: str,
    timestamp: datetime,
    tg_entities: Optional[list] = None,
) -> tuple[str, Optional[dict]]:
    """
    Build a polished Rubika message using the Metadata API.

    Final layout (no raw markdown chars anywhere):
        ―――――――――――――――――――――――――
        📡  Channel Name          ← Bold via metadata
        🕐  2026-05-16 · 11:30    ← Italic via metadata
        ―――――――――――――――――――――――――
        body text…
        vpn://config              ← Mono + Quote via metadata

    Returns (plain_text, metadata_dict | None)
    """
    ts_str = timestamp.strftime("%Y-%m-%d · %H:%M UTC")

    # Prefixes — plain chars, no markdown
    ch_pre = "📡  "
    ts_pre = "🕐  "
    div    = DIVIDER

    # Assemble header as a single string
    # Format:  <div>\n<ch_pre><name>\n<ts_pre><ts>\n<div>
    header = f"{div}\n{ch_pre}{channel_name}\n{ts_pre}{ts_str}\n{div}"

    meta: list[dict] = []
    cur = 0   # UTF-16 cursor through full message

    # div + \n
    cur += _utf16_len(div) + 1

    # ch_pre then channel_name (Bold)
    cur += _utf16_len(ch_pre)
    ch_u16 = _utf16_len(channel_name)
    if ch_u16:
        meta.append({"type": "Bold", "from_index": cur, "length": ch_u16})
    cur += ch_u16 + 1   # +1 for \n

    # ts_pre then ts_str (Italic)
    cur += _utf16_len(ts_pre)
    ts_u16 = _utf16_len(ts_str)
    if ts_u16:
        meta.append({"type": "Italic", "from_index": cur, "length": ts_u16})
    cur += ts_u16 + 1   # +1 for \n

    # second div (no trailing \n here — the \n before body counts below)
    cur += _utf16_len(div)

    # Body (only if non-empty)
    if raw_text and raw_text.strip():
        # separator \n between header and body
        cur += 1
        body_plain, body_meta = _build_body(raw_text, tg_entities or [], cur)
        full_text = header + "\n" + body_plain
        meta.extend(body_meta)
    else:
        full_text = header

    # Rubika hard cap: 30 metadata parts
    meta = meta[:30]
    metadata = {"meta_data_parts": meta} if meta else None
    return full_text, metadata


# ══════════════════════════════════════════════════════════════
# WELCOME MESSAGE
# ══════════════════════════════════════════════════════════════

def _make_welcome() -> tuple[str, dict]:
    """Build welcome message text + correctly computed Rubika metadata."""
    div = "―――――――――――――――――――――――――"
    title_text  = "خوش آمدید به ربات VPN"
    footer_text = "اتصال امن  •  اینترنت آزاد  🚀"

    lines = [
        div,
        f"🌐  {title_text}",
        div,
        "",
        "سلام! 👋 شما با موفقیت عضو شدید.",
        "",
        "✅ از این پس بهترین کانفیگ‌های VPN",
        "و پروکسی را مستقیماً اینجا دریافت می‌کنید.",
        "",
        "📌 نحوه استفاده:",
        "• کانفیگ‌ها با فرمت‌بندی ویژه ارسال می‌شوند",
        "• روی هر لینک ضربه بزنید تا کپی شود",
        "• آپدیت‌ها بلافاصله پس از انتشار می‌رسند",
        "",
        div,
        f"🔐  {footer_text}",
    ]
    full = "\n".join(lines)

    def u16(s): return len(s.encode("utf-16-le")) // 2

    # Find UTF-16 offset of title_text inside the full string
    title_line = f"🌐  {title_text}"
    pre_title  = "\n".join(lines[:1]) + "\n" + "🌐  "
    off_title  = u16(pre_title)
    len_title  = u16(title_text)

    # Find UTF-16 offset of footer_text
    pre_footer = "\n".join(lines[:-1]) + "\n" + "🔐  "
    off_footer = u16(pre_footer)
    len_footer = u16(footer_text)

    meta = {"meta_data_parts": [
        {"type": "Bold",   "from_index": off_title,  "length": len_title},
        {"type": "Italic", "from_index": off_footer, "length": len_footer},
    ]}
    return full, meta

WELCOME_TEXT, WELCOME_META = _make_welcome()


# ══════════════════════════════════════════════════════════════
# SUBSCRIBER POLLING
# ══════════════════════════════════════════════════════════════

async def poll_subscribers(session: aiohttp.ClientSession):
    global update_offset, subscribers
    payload = {"limit": 100}
    if update_offset:
        payload["offset_id"] = update_offset

    resp = await rubika_post("getUpdates", payload, session)
    if not resp or "data" not in resp:
        return

    data   = resp["data"]
    updates = data.get("updates", [])
    update_offset = data.get("next_offset_id", update_offset)

    changed = False
    for upd in updates:
        upd_type = upd.get("type", "")
        chat_id  = upd.get("chat_id", "")
        if not chat_id:
            continue

        is_new = False
        if upd_type == "StartedBot":
            is_new = True
        elif upd_type == "NewMessage":
            msg = upd.get("new_message", {})
            txt = (msg.get("text") or "").strip().lower()
            btn = (msg.get("aux_data") or {}).get("button_id", "")
            if txt in ("/start", "start") or btn == "start":
                is_new = True

        if is_new and chat_id not in subscribers:
            log.info("New subscriber: %s", chat_id)
            subscribers.add(chat_id)
            changed = True
            await rubika_send_message(chat_id, WELCOME_TEXT, session, metadata=WELCOME_META)

    if changed:
        save_subscribers()
        git_push("add subscriber(s)")


# ══════════════════════════════════════════════════════════════
# REACTION UPDATES
# ══════════════════════════════════════════════════════════════

REACTION_SCHEDULE = [3*60, 5*60, 10*60, 15*60, 25*60, 40*60, 60*60, 90*60, 120*60]


async def schedule_reaction_update(tg_client: TelegramClient,
                                    rubika_pairs: list,
                                    tg_channel, tg_msg_id: int,
                                    original_text: str,
                                    original_metadata: Optional[dict],
                                    session: aiohttp.ClientSession):
    """
    Background task: fetch TG reactions periodically and edit Rubika messages
    to append emoji counts. CRITICAL: resend original_metadata on every edit —
    editMessageText wipes all formatting (Mono/Quote/Bold) unless metadata is
    explicitly included again.
    """
    send_time = time.monotonic()

    for delay in REACTION_SCHEDULE:
        remaining = delay - (time.monotonic() - send_time)
        if remaining > 0:
            await asyncio.sleep(remaining)
        try:
            msgs = await tg_client.get_messages(tg_channel, ids=[tg_msg_id])
            if not msgs or not msgs[0]:
                break
            msg = msgs[0]
            reactions = getattr(msg, "reactions", None)
            if reactions and reactions.results:
                top = sorted(reactions.results, key=lambda r: r.count, reverse=True)[:3]
                reaction_str = "  ".join(
                    f"{getattr(r.reaction, 'emoticon', '❤️')} {r.count}"
                    for r in top
                )
                new_text = original_text + f"\n\n{reaction_str}"
            else:
                new_text = original_text

            for chat_id, msg_id in rubika_pairs:
                # Always resend original_metadata — without it, Rubika strips
                # all formatting (Mono, Quote, Bold) from the edited message
                await rubika_edit_message(chat_id, msg_id, new_text, session,
                                          metadata=original_metadata)
            log.debug("Reaction edit done for tg_msg %s", tg_msg_id)
        except Exception as e:
            log.debug("Reaction update error: %s", e)
            break


# ══════════════════════════════════════════════════════════════
# TELEGRAM MEDIA → RUBIKA
# ══════════════════════════════════════════════════════════════

def _detect_file_type(media) -> str:
    """Return Rubika FileTypeEnum string."""
    if isinstance(media, MessageMediaPhoto):
        return "Image"
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                if getattr(attr, "round_message", False):
                    return "Video"
                return "Video"
            if isinstance(attr, DocumentAttributeAudio):
                if getattr(attr, "voice", False):
                    return "Voice"
                return "Music"
        mime = getattr(doc, "mime_type", "")
        if mime.startswith("image/gif") or mime == "video/mp4":
            return "Gif"
        return "File"
    return "File"


async def forward_media(tg_client: TelegramClient, tg_msg,
                         caption: str, session: aiohttp.ClientSession) -> list:
    """Download from Telegram, upload to Rubika, broadcast. Returns sent pairs."""
    media     = tg_msg.media
    file_type = _detect_file_type(media)
    limit     = SIZE_LIMITS.get(file_type, SIZE_LIMITS["File"])

    # Check size before downloading
    if isinstance(media, MessageMediaDocument):
        size = getattr(media.document, "size", 0) or 0
        if size > limit:
            note = (
                f"⚠️ فایل دریافتی ({file_type}) بزرگ‌تر از حد مجاز "
                f"({limit // 1024 // 1024} MB) است و ارسال نشد.\n\n"
                + caption
            )
            return await broadcast_text(note, session)

    log.info("Downloading %s …", file_type)
    try:
        file_bytes = await tg_client.download_media(tg_msg, bytes)
    except Exception as e:
        log.error("Telegram download failed: %s", e)
        return []

    if len(file_bytes) > limit:
        note = (
            f"⚠️ فایل ({file_type}) پس از دانلود از حد مجاز عبور کرد و ارسال نشد.\n\n"
            + caption
        )
        return await broadcast_text(note, session)

    log.info("Uploading %d bytes as %s …", len(file_bytes), file_type)
    file_id = await rubika_upload_file(file_bytes, file_type, session)
    if not file_id:
        log.error("Upload failed — skipping media")
        return []

    pairs = await broadcast_file(file_id, caption, session)
    log.info("Sent media %s to %d subscribers", file_type, len(pairs))
    return pairs


# ══════════════════════════════════════════════════════════════
# CATCH-UP (missed messages on startup)
# ══════════════════════════════════════════════════════════════

async def catch_up(tg_client: TelegramClient, channels: list,
                   state: dict, session: aiohttp.ClientSession):
    log.info("Running catch-up for %d channels …", len(channels))
    for ch in channels:
        try:
            entity  = await tg_client.get_entity(ch)
            ch_name = _channel_display(entity)
            last_id = state.get(str(ch), 0)
            msgs    = await tg_client.get_messages(entity, limit=10)
            msgs    = sorted(msgs, key=lambda m: m.id)
            for msg in msgs:
                if msg.id <= last_id:
                    continue
                await handle_tg_message(tg_client, msg, entity, ch_name, session)
                state[str(ch)] = msg.id
        except Exception as e:
            log.warning("Catch-up failed for %s: %s", ch, e)
    save_state(state)
    log.info("Catch-up complete ✓")


# ══════════════════════════════════════════════════════════════
# CORE MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════

async def handle_tg_message(tg_client: TelegramClient, msg,
                              entity, channel_name: str,
                              session: aiohttp.ClientSession):
    if not subscribers:
        return

    ts          = msg.date.astimezone(timezone.utc)
    raw_text    = msg.text or msg.message or ""
    tg_entities = msg.entities or []

    # format_message uses Telethon entities (not regex) for Bold/Italic/Mono
    caption, metadata = format_message(raw_text, channel_name, ts, tg_entities)

    if msg.media:
        # Media: send caption as plain text — metadata offsets don't apply to file captions
        pairs = await forward_media(tg_client, msg, caption, session)
    else:
        if not raw_text.strip():
            return
        pairs = await broadcast_text(caption, session, metadata=metadata)
        log.info("Forwarded text to %d subscribers", len(pairs))

    # Schedule reaction updates in background (text messages only)
    if pairs and (msg.media is None):
        asyncio.create_task(
            schedule_reaction_update(
                tg_client, pairs, entity, msg.id,
                caption,    # plain text — resent on every edit
                metadata,   # original formatting — must be resent or Mono/Quote vanish
                session,
            )
        )


# ══════════════════════════════════════════════════════════════
# ADMIN NOTIFICATION
# ══════════════════════════════════════════════════════════════

async def notify_admin(text: str, session: aiohttp.ClientSession):
    if ADMIN_CHAT_ID:
        await rubika_send_message(ADMIN_CHAT_ID, text, session)


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

async def main():
    # ── Git clone data repo ──
    if DATA_REPO_DIR.exists():
        log.info("data_repo already exists, skipping clone")
    else:
        git_clone()

    load_subscribers()
    state = load_state()

    # ── Load channels ──
    channels_path = Path(CHANNELS_FILE)
    if not channels_path.exists():
        log.critical("channels.json not found!")
        sys.exit(1)
    channels: list = json.loads(channels_path.read_text())
    log.info("Monitoring %d channels: %s", len(channels), channels)

    # ── Start Telegram client ──
    tg_client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    await tg_client.start()
    log.info("Telegram client connected ✓")

    async with aiohttp.ClientSession() as session:
        await notify_admin("🟢 ربات فوروارد راه‌اندازی شد.", session)

        # ── Catch-up ──
        await catch_up(tg_client, channels, state, session)

        # ── Register Telegram event handler ──
        resolved_entities = {}
        for ch in channels:
            try:
                ent = await tg_client.get_entity(ch)
                resolved_entities[ent.id] = ent
            except Exception as e:
                log.warning("Could not resolve %s: %s", ch, e)

        @tg_client.on(events.NewMessage(chats=list(resolved_entities.keys())))
        async def on_new_message(event):
            msg    = event.message
            entity = resolved_entities.get(event.chat_id)
            ch_name = _channel_display(entity)
            state[str(event.chat_id)] = msg.id
            save_state(state)
            await handle_tg_message(tg_client, msg, entity, ch_name, session)

        # ── Background: subscriber polling ──
        async def subscriber_loop():
            while True:
                try:
                    await poll_subscribers(session)
                except Exception as e:
                    log.error("Subscriber poll error: %s", e)
                await asyncio.sleep(POLL_INTERVAL)

        poll_task = asyncio.create_task(subscriber_loop())

        # ── Run until RUN_DURATION ──
        log.info("Bot running for %d seconds …", RUN_DURATION)
        try:
            await asyncio.sleep(RUN_DURATION)
        except asyncio.CancelledError:
            pass
        finally:
            poll_task.cancel()
            log.info("Shutting down …")
            save_subscribers()
            save_state(state)
            git_push("final state on shutdown")
            await tg_client.disconnect()
            await notify_admin("🔴 ربات فوروارد متوقف شد.", session)
            log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
