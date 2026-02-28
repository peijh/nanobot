"""Matrix (Element) channel — inbound sync + outbound message/media delivery."""

import asyncio
import base64
import json
import logging
import mimetypes
import time
from pathlib import Path
from typing import Any, TypeAlias

from loguru import logger

try:
    import nh3
    from mistune import create_markdown
    from nio import (
        AsyncClient, AsyncClientConfig, ContentRepositoryConfigError,
        DownloadError, InviteEvent, JoinError, LocalProtocolError,
        MatrixRoom, MemoryDownloadResponse,
        RoomEncryptedMedia, RoomMessage, RoomMessageMedia, RoomMessageText,
        RoomSendError, RoomTypingError, SyncError, UploadError,
    )
    from nio.crypto.attachments import decrypt_attachment
    from nio.exceptions import EncryptionError
except ImportError as e:
    raise ImportError(
        "Matrix dependencies not installed. Run: pip install nanobot-ai[matrix]"
    ) from e

try:
    from olm.pk import PkSigning as _PkSigning
except ImportError:
    _PkSigning = None

from nanobot.bus.events import OutboundMessage
from nanobot.channels.base import BaseChannel
from nanobot.config.loader import get_data_dir
from nanobot.utils.helpers import safe_filename

TYPING_NOTICE_TIMEOUT_MS = 30_000
# Must stay below TYPING_NOTICE_TIMEOUT_MS so the indicator doesn't expire mid-processing.
TYPING_KEEPALIVE_INTERVAL_MS = 20_000
MATRIX_HTML_FORMAT = "org.matrix.custom.html"
_ATTACH_MARKER = "[attachment: {}]"
_ATTACH_TOO_LARGE = "[attachment: {} - too large]"
_ATTACH_FAILED = "[attachment: {} - download failed]"
_ATTACH_UPLOAD_FAILED = "[attachment: {} - upload failed]"
_DEFAULT_ATTACH_NAME = "attachment"
_CROSS_SIGNING_SEEDS_FILE = "cross_signing_seeds.json"
_MSGTYPE_MAP = {"m.image": "image", "m.audio": "audio", "m.video": "video", "m.file": "file"}
STREAMING_EDIT_MIN_INTERVAL_S = 0.8  # minimum seconds between edits to avoid rate-limiting

MATRIX_MEDIA_EVENT_FILTER = (RoomMessageMedia, RoomEncryptedMedia)
MatrixMediaEvent: TypeAlias = RoomMessageMedia | RoomEncryptedMedia

MATRIX_MARKDOWN = create_markdown(
    escape=True,
    plugins=["table", "strikethrough", "url", "superscript", "subscript"],
)

MATRIX_ALLOWED_HTML_TAGS = {
    "p", "a", "strong", "em", "del", "code", "pre", "blockquote",
    "ul", "ol", "li", "h1", "h2", "h3", "h4", "h5", "h6",
    "hr", "br", "table", "thead", "tbody", "tr", "th", "td",
    "caption", "sup", "sub", "img",
}
MATRIX_ALLOWED_HTML_ATTRIBUTES: dict[str, set[str]] = {
    "a": {"href"}, "code": {"class"}, "ol": {"start"},
    "img": {"src", "alt", "title", "width", "height"},
    "th": {"align", "colspan", "rowspan"},
    "td": {"align", "colspan", "rowspan"},
    "table": {"border"},
}
MATRIX_ALLOWED_URL_SCHEMES = {"https", "http", "matrix", "mailto", "mxc"}


def _filter_matrix_html_attribute(tag: str, attr: str, value: str) -> str | None:
    """Filter attribute values to a safe Matrix-compatible subset."""
    if tag == "a" and attr == "href":
        return value if value.lower().startswith(("https://", "http://", "matrix:", "mailto:")) else None
    if tag == "img" and attr == "src":
        return value if value.lower().startswith("mxc://") else None
    if tag == "code" and attr == "class":
        classes = [c for c in value.split() if c.startswith("language-") and not c.startswith("language-_")]
        return " ".join(classes) if classes else None
    return value


MATRIX_HTML_CLEANER = nh3.Cleaner(
    tags=MATRIX_ALLOWED_HTML_TAGS,
    attributes=MATRIX_ALLOWED_HTML_ATTRIBUTES,
    attribute_filter=_filter_matrix_html_attribute,
    url_schemes=MATRIX_ALLOWED_URL_SCHEMES,
    strip_comments=True,
    link_rel="noopener noreferrer",
)

_TABLE_BLOCK_RE = __import__("re").compile(r'<table\b[^>]*>.*?</table>', __import__("re").DOTALL | __import__("re").IGNORECASE)


def _html_table_to_ascii(table_html: str) -> str:
    """Parse an HTML <table> and return an ASCII art text representation."""
    from html.parser import HTMLParser
    import html as _html_mod

    class _Parser(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.rows: list[list[str]] = []
            self._cur_row: list[str] | None = None
            self._cur_cell: str | None = None
            self._header_end: int = 0
            self._in_thead: bool = False

        def handle_starttag(self, tag: str, attrs: list) -> None:
            if tag == "thead":
                self._in_thead = True
            elif tag == "tr":
                self._cur_row = []
            elif tag in ("td", "th"):
                self._cur_cell = ""

        def handle_endtag(self, tag: str) -> None:
            if tag == "thead":
                self._in_thead = False
                self._header_end = len(self.rows)
            elif tag == "tr":
                if self._cur_row is not None:
                    self.rows.append(self._cur_row)
                self._cur_row = None
            elif tag in ("td", "th"):
                if self._cur_row is not None and self._cur_cell is not None:
                    self._cur_row.append(self._cur_cell.strip())
                self._cur_cell = None

        def handle_data(self, data: str) -> None:
            if self._cur_cell is not None:
                self._cur_cell += data

        def handle_entityref(self, name: str) -> None:
            if self._cur_cell is not None:
                self._cur_cell += _html_mod.unescape(f"&{name};")

        def handle_charref(self, name: str) -> None:
            if self._cur_cell is not None:
                self._cur_cell += _html_mod.unescape(f"&#{name};")

    p = _Parser()
    p.feed(table_html)
    if not p.rows:
        return ""

    col_n = max(len(r) for r in p.rows)
    widths = [max((len(r[i]) if i < len(r) else 0) for r in p.rows) for i in range(col_n)]
    widths = [max(w, 3) for w in widths]

    sep  = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    hsep = "+" + "+".join("=" * (w + 2) for w in widths) + "+"

    lines = [sep]
    for i, row in enumerate(p.rows):
        cells = [f" {(row[j] if j < len(row) else ''):{widths[j]}} " for j in range(col_n)]
        lines.append("|" + "|".join(cells) + "|")
        if i == p._header_end - 1:
            lines.append(hsep)
        else:
            lines.append(sep)
    return "\n".join(lines)


def _html_tables_to_pre(html_text: str) -> str:
    """Replace <table>…</table> blocks with ASCII art in <pre><code> blocks."""
    import html as _html_mod

    def _replace(m: __import__("re").Match) -> str:  # type: ignore[type-arg]
        ascii_text = _html_table_to_ascii(m.group(0))
        if not ascii_text:
            return m.group(0)
        return f"<pre><code>{_html_mod.escape(ascii_text)}</code></pre>"

    return _TABLE_BLOCK_RE.sub(_replace, html_text)


def _render_markdown_html(text: str) -> str | None:
    """Render markdown to sanitized HTML; returns None for plain text."""
    try:
        raw = MATRIX_MARKDOWN(text)
        # Convert tables to ASCII art <pre> blocks before sanitizing —
        # Element X and other mobile clients don't render HTML <table> well.
        raw = _html_tables_to_pre(raw)
        formatted = MATRIX_HTML_CLEANER.clean(raw).strip()
    except Exception:
        return None
    if not formatted:
        return None
    # Skip formatted_body for plain <p>text</p> to keep payload minimal.
    if formatted.startswith("<p>") and formatted.endswith("</p>"):
        inner = formatted[3:-4]
        if "<" not in inner and ">" not in inner:
            return None
    return formatted


def _build_matrix_text_content(text: str) -> dict[str, object]:
    """Build Matrix m.text payload with optional HTML formatted_body."""
    content: dict[str, object] = {"msgtype": "m.text", "body": text, "m.mentions": {}}
    if html := _render_markdown_html(text):
        content["format"] = MATRIX_HTML_FORMAT
        content["formatted_body"] = html
    return content


class _NioLoguruHandler(logging.Handler):
    """Route matrix-nio stdlib logs into Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame, depth = frame.f_back, depth + 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def _configure_nio_logging_bridge() -> None:
    """Bridge matrix-nio logs to Loguru (idempotent)."""
    nio_logger = logging.getLogger("nio")
    if not any(isinstance(h, _NioLoguruHandler) for h in nio_logger.handlers):
        nio_logger.handlers = [_NioLoguruHandler()]
        nio_logger.propagate = False


class MatrixChannel(BaseChannel):
    """Matrix (Element) channel using long-polling sync."""

    name = "matrix"

    def __init__(self, config: Any, bus, *, restrict_to_workspace: bool = False,
                 workspace: Path | None = None):
        super().__init__(config, bus)
        self.client: AsyncClient | None = None
        self._sync_task: asyncio.Task | None = None
        self._typing_tasks: dict[str, asyncio.Task] = {}
        self._restrict_to_workspace = restrict_to_workspace
        self._workspace = workspace.expanduser().resolve() if workspace else None
        self._server_upload_limit_bytes: int | None = None
        self._server_upload_limit_checked = False
        self._streaming_sessions: dict[str, dict[str, Any]] = {}
        self._initial_sync_done = False

    async def start(self) -> None:
        """Start Matrix client and begin sync loop."""
        self._running = True
        _configure_nio_logging_bridge()

        store_path = get_data_dir() / "matrix-store"
        store_path.mkdir(parents=True, exist_ok=True)

        self.client = AsyncClient(
            homeserver=self.config.homeserver, user=self.config.user_id,
            store_path=store_path,
            config=AsyncClientConfig(store_sync_tokens=True, encryption_enabled=self.config.e2ee_enabled),
        )
        self.client.user_id = self.config.user_id
        self.client.access_token = self.config.access_token
        self.client.device_id = self.config.device_id

        # Register response-level callbacks (errors) early, but NOT event callbacks yet.
        self._register_response_callbacks()

        if not self.config.e2ee_enabled:
            logger.warning("Matrix E2EE disabled; encrypted rooms may be undecryptable.")

        if self.config.device_id:
            try:
                self.client.load_store()
            except Exception:
                logger.exception("Matrix store load failed; restart may replay recent messages.")
        else:
            logger.warning("Matrix device_id empty; restart may replay recent messages.")

        # Always upload device keys after loading store.
        # Critical after store deletion: publishes new Olm identity keys so other
        # clients can establish Olm sessions and share Megolm keys with us.
        if self.config.e2ee_enabled:
            try:
                resp = await self.client.keys_upload()
                if isinstance(resp, (Exception,)):
                    logger.warning("Matrix device key upload response: {}", resp)
                else:
                    logger.info("Matrix device keys uploaded successfully")
            except LocalProtocolError:
                logger.debug("Matrix device keys already up to date")
            except Exception:
                logger.opt(exception=True).warning("Matrix device key upload failed")

        if self.config.e2ee_enabled:
            await self._ensure_cross_signing()

        # Perform a catch-up sync to skip old messages before we start processing.
        # This prevents a flood of undecryptable historical Megolm events after a
        # store reset. Only new messages arriving after this point will be handled.
        if not self.client.next_batch:
            logger.info("Matrix: performing initial catch-up sync (skipping history)...")
            try:
                resp = await self.client.sync(
                    timeout=10000, full_state=True,
                    sync_filter={"room": {"timeline": {"limit": 1}}},
                )
                if not isinstance(resp, SyncError):
                    logger.info("Matrix: initial sync done, now listening for new messages")
                else:
                    logger.warning("Matrix: initial sync returned error: {}", resp)
                # After initial sync, upload one-time keys that the server may now request.
                if self.config.e2ee_enabled:
                    try:
                        await self.client.keys_upload()
                    except LocalProtocolError:
                        pass
            except Exception:
                logger.opt(exception=True).warning("Matrix: initial catch-up sync failed")

        # NOW register event callbacks so only new messages trigger processing.
        self._register_event_callbacks()
        self._initial_sync_done = True

        self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop(self) -> None:
        """Stop the Matrix channel with graceful sync shutdown."""
        self._running = False
        for room_id, session in list(self._streaming_sessions.items()):
            if task := session.get("flush_task"):
                task.cancel()
        self._streaming_sessions.clear()
        for room_id in list(self._typing_tasks):
            await self._stop_typing_keepalive(room_id, clear_typing=False)
        if self.client:
            self.client.stop_sync_forever()
        if self._sync_task:
            try:
                await asyncio.wait_for(asyncio.shield(self._sync_task),
                                       timeout=self.config.sync_stop_grace_seconds)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._sync_task.cancel()
                try:
                    await self._sync_task
                except asyncio.CancelledError:
                    pass
        if self.client:
            await self.client.close()

    # ---- Cross-signing bootstrap ----

    async def _ensure_cross_signing(self) -> None:
        """Bootstrap cross-signing so the bot device appears verified to others."""
        if not self.client or not self.config.e2ee_enabled:
            return
        if _PkSigning is None:
            logger.debug("olm.pk.PkSigning not available; skipping cross-signing bootstrap")
            return
        if not self.config.device_id or not getattr(self.client, "olm", None):
            return
        try:
            store_path = get_data_dir() / "matrix-store"
            seeds_path = store_path / _CROSS_SIGNING_SEEDS_FILE
            await self._cross_signing_bootstrap(seeds_path)
        except Exception:
            logger.opt(exception=True).debug("Cross-signing bootstrap failed")
            logger.warning(
                "Could not auto-verify bot device. "
                "Verify manually from Element or set matrix.password in config."
            )

    @staticmethod
    def _canonical_json(obj: Any) -> str:
        """Matrix canonical JSON: sorted keys, compact, UTF-8."""
        return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

    @classmethod
    def _sign_json(cls, signing_key, obj: dict) -> str:
        """Sign a JSON object (excluding 'signatures' and 'unsigned' keys)."""
        to_sign = {k: v for k, v in obj.items() if k not in ("signatures", "unsigned")}
        return signing_key.sign(cls._canonical_json(to_sign))

    def _load_cross_signing_seeds(self, seeds_path: Path) -> dict:
        """Load saved cross-signing seeds or generate fresh ones."""
        if seeds_path.exists():
            try:
                data = json.loads(seeds_path.read_text())
                if all(k in data for k in ("master", "self_signing", "user_signing")):
                    return data
            except Exception:
                pass
        seeds = {
            "master": base64.b64encode(_PkSigning.generate_seed()).decode(),
            "self_signing": base64.b64encode(_PkSigning.generate_seed()).decode(),
            "user_signing": base64.b64encode(_PkSigning.generate_seed()).decode(),
            "_generated": True,
        }
        self._save_cross_signing_seeds(seeds_path, seeds)
        return seeds

    @staticmethod
    def _save_cross_signing_seeds(seeds_path: Path, seeds: dict) -> None:
        try:
            seeds_path.write_text(json.dumps(seeds))
            seeds_path.chmod(0o600)
        except OSError:
            logger.warning("Could not persist cross-signing seeds to {}", seeds_path)

    async def _cross_signing_bootstrap(self, seeds_path: Path) -> None:
        """Upload cross-signing keys and self-sign the bot device."""
        import aiohttp

        user_id = self.config.user_id
        device_id = self.config.device_id
        homeserver = self.config.homeserver.rstrip("/")
        headers = {
            "Authorization": f"Bearer {self.config.access_token}",
            "Content-Type": "application/json",
        }

        seeds_data = self._load_cross_signing_seeds(seeds_path)
        master = _PkSigning(base64.b64decode(seeds_data["master"]))
        self_signing = _PkSigning(base64.b64decode(seeds_data["self_signing"]))
        user_signing = _PkSigning(base64.b64decode(seeds_data["user_signing"]))

        async with aiohttp.ClientSession() as http:
            # Check if cross-signing is already set up on the server
            query_url = f"{homeserver}/_matrix/client/v3/keys/query"
            async with http.post(query_url, json={"device_keys": {user_id: []}}, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    existing_master = data.get("master_keys", {}).get(user_id)
                    if existing_master:
                        existing_pub = next(iter(existing_master.get("keys", {}).values()), None)
                        if existing_pub == master.public_key:
                            # Our seeds match — just ensure device is self-signed
                            logger.debug("Cross-signing keys already on server; verifying device")
                            try:
                                await self.client.keys_upload()
                            except LocalProtocolError:
                                pass  # no new device keys to upload — that's fine
                            await self._self_sign_device(http, homeserver, headers, user_id, device_id, self_signing)
                            return
                        elif seeds_data.get("_generated"):
                            # Seeds don't match server. If we have a password, re-upload our new
                            # cross-signing keys (e.g. after store deletion). Otherwise give up.
                            password = getattr(self.config, "password", None) or ""
                            if not password:
                                logger.info(
                                    "Cross-signing already set up by another session. "
                                    "Set matrix.password in config or verify this device manually."
                                )
                                return
                            logger.info(
                                "Cross-signing seed mismatch (store was reset); "
                                "re-uploading cross-signing keys with password auth."
                            )

            # Build key payloads
            master_key_payload = {
                "user_id": user_id, "usage": ["master"],
                "keys": {f"ed25519:{master.public_key}": master.public_key},
            }
            ss_payload = {
                "user_id": user_id, "usage": ["self_signing"],
                "keys": {f"ed25519:{self_signing.public_key}": self_signing.public_key},
            }
            ss_payload["signatures"] = {
                user_id: {f"ed25519:{master.public_key}": self._sign_json(master, ss_payload)}
            }
            us_payload = {
                "user_id": user_id, "usage": ["user_signing"],
                "keys": {f"ed25519:{user_signing.public_key}": user_signing.public_key},
            }
            us_payload["signatures"] = {
                user_id: {f"ed25519:{master.public_key}": self._sign_json(master, us_payload)}
            }

            upload_body: dict[str, Any] = {
                "master_key": master_key_payload,
                "self_signing_key": ss_payload,
                "user_signing_key": us_payload,
            }

            # Upload cross-signing keys (may require UIA)
            upload_url = f"{homeserver}/_matrix/client/v3/keys/device_signing/upload"
            uploaded = False

            async with http.post(upload_url, json=upload_body, headers=headers) as resp:
                if resp.status == 200:
                    uploaded = True
                elif resp.status == 401:
                    uia_data = await resp.json()
                    session_id = uia_data.get("session", "")
                    flows = uia_data.get("flows", [])
                    available_types: set[str] = set()
                    for flow in flows:
                        for stage in flow.get("stages", []):
                            available_types.add(stage)

                    auth: dict[str, Any] | None = None
                    password = getattr(self.config, "password", None) or ""
                    if password and "m.login.password" in available_types:
                        auth = {
                            "type": "m.login.password",
                            "identifier": {"type": "m.id.user", "user": user_id},
                            "password": password,
                            "session": session_id,
                        }
                    elif "m.login.dummy" in available_types or not available_types:
                        auth = {"type": "m.login.dummy", "session": session_id}
                    else:
                        logger.info(
                            "Cross-signing UIA requires password auth. "
                            "Set matrix.password in config for auto-verification."
                        )
                        return

                    upload_body["auth"] = auth
                    async with http.post(upload_url, json=upload_body, headers=headers) as resp2:
                        if resp2.status == 200:
                            uploaded = True
                        else:
                            body = await resp2.text()
                            logger.warning("Cross-signing upload failed (UIA): {} {}", resp2.status, body[:200])
                            return
                else:
                    body = await resp.text()
                    logger.warning("Cross-signing upload failed: {} {}", resp.status, body[:200])
                    return

            if uploaded:
                logger.info("Cross-signing keys uploaded for {}", user_id)
                seeds_data.pop("_generated", None)
                self._save_cross_signing_seeds(seeds_path, seeds_data)
                try:
                    await self.client.keys_upload()
                except LocalProtocolError:
                    pass  # no new device keys to upload — that's fine
                await self._self_sign_device(http, homeserver, headers, user_id, device_id, self_signing)

    async def _self_sign_device(
        self, http, homeserver: str, headers: dict,
        user_id: str, device_id: str, self_signing,
    ) -> None:
        """Sign the bot's device key with the self-signing key."""
        identity_keys = self.client.olm.account.identity_keys
        device_payload = {
            "user_id": user_id,
            "device_id": device_id,
            "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
            "keys": {
                f"curve25519:{device_id}": identity_keys["curve25519"],
                f"ed25519:{device_id}": identity_keys["ed25519"],
            },
        }
        device_sig = self._sign_json(self_signing, device_payload)
        sig_body = {
            user_id: {
                device_id: {
                    **device_payload,
                    "signatures": {
                        user_id: {
                            f"ed25519:{self_signing.public_key}": device_sig,
                        }
                    },
                }
            }
        }
        sig_url = f"{homeserver}/_matrix/client/v3/keys/signatures/upload"
        async with http.post(sig_url, json=sig_body, headers=headers) as resp:
            if resp.status == 200:
                logger.info("Bot device {} self-signed; should now appear as verified", device_id)
            else:
                body = await resp.text()
                logger.warning("Device signature upload failed: {} {}", resp.status, body[:200])

    def _is_workspace_path_allowed(self, path: Path) -> bool:
        """Check path is inside workspace (when restriction enabled)."""
        if not self._restrict_to_workspace or not self._workspace:
            return True
        try:
            path.resolve(strict=False).relative_to(self._workspace)
            return True
        except ValueError:
            return False

    def _collect_outbound_media_candidates(self, media: list[str]) -> list[Path]:
        """Deduplicate and resolve outbound attachment paths."""
        seen: set[str] = set()
        candidates: list[Path] = []
        for raw in media:
            if not isinstance(raw, str) or not raw.strip():
                continue
            path = Path(raw.strip()).expanduser()
            try:
                key = str(path.resolve(strict=False))
            except OSError:
                key = str(path)
            if key not in seen:
                seen.add(key)
                candidates.append(path)
        return candidates

    @staticmethod
    def _build_outbound_attachment_content(
        *, filename: str, mime: str, size_bytes: int,
        mxc_url: str, encryption_info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build Matrix content payload for an uploaded file/image/audio/video."""
        prefix = mime.split("/")[0]
        msgtype = {"image": "m.image", "audio": "m.audio", "video": "m.video"}.get(prefix, "m.file")
        content: dict[str, Any] = {
            "msgtype": msgtype, "body": filename, "filename": filename,
            "info": {"mimetype": mime, "size": size_bytes}, "m.mentions": {},
        }
        if encryption_info:
            content["file"] = {**encryption_info, "url": mxc_url}
        else:
            content["url"] = mxc_url
        return content

    def _is_encrypted_room(self, room_id: str) -> bool:
        if not self.client:
            return False
        room = getattr(self.client, "rooms", {}).get(room_id)
        return bool(getattr(room, "encrypted", False))

    async def _send_room_content(self, room_id: str, content: dict[str, Any]) -> str | None:
        """Send m.room.message with E2EE options. Returns event_id on success."""
        if not self.client:
            return None
        kwargs: dict[str, Any] = {"room_id": room_id, "message_type": "m.room.message", "content": content}
        if self.config.e2ee_enabled:
            kwargs["ignore_unverified_devices"] = True
        response = await self.client.room_send(**kwargs)
        if isinstance(response, RoomSendError):
            return None
        return getattr(response, "event_id", None)

    async def _resolve_server_upload_limit_bytes(self) -> int | None:
        """Query homeserver upload limit once per channel lifecycle."""
        if self._server_upload_limit_checked:
            return self._server_upload_limit_bytes
        self._server_upload_limit_checked = True
        if not self.client:
            return None
        try:
            response = await self.client.content_repository_config()
        except Exception:
            return None
        upload_size = getattr(response, "upload_size", None)
        if isinstance(upload_size, int) and upload_size > 0:
            self._server_upload_limit_bytes = upload_size
            return upload_size
        return None

    @staticmethod
    def _build_edit_content(original_event_id: str, text: str) -> dict[str, Any]:
        """Build m.replace edit payload for progressive streaming updates."""
        new_content = _build_matrix_text_content(text)
        content: dict[str, Any] = {
            "msgtype": "m.text",
            "body": f"* {text}",
            "m.new_content": new_content,
            "m.relates_to": {
                "rel_type": "m.replace",
                "event_id": original_event_id,
            },
            "m.mentions": {},
        }
        if html := _render_markdown_html(text):
            content["format"] = MATRIX_HTML_FORMAT
            content["formatted_body"] = f"* {html}"
        return content

    async def _flush_streaming_edit(self, room_id: str) -> None:
        """Flush pending streaming content after throttle interval elapses."""
        session = self._streaming_sessions.get(room_id)
        if not session or session.get("pending_content") is None:
            return
        remaining = max(0, STREAMING_EDIT_MIN_INTERVAL_S - (time.monotonic() - session["last_edit_time"]))
        if remaining > 0:
            await asyncio.sleep(remaining)
        session = self._streaming_sessions.get(room_id)
        if not session or session.get("pending_content") is None:
            return
        text = session["pending_content"]
        if text != session["last_content"]:
            try:
                await self._send_room_content(
                    room_id, self._build_edit_content(session["event_id"], text),
                )
                session["last_content"] = text
                session["last_edit_time"] = time.monotonic()
            except Exception:
                pass
        session["pending_content"] = None

    async def _effective_media_limit_bytes(self) -> int:
        """min(local config, server advertised) — 0 blocks all uploads."""
        local_limit = max(int(self.config.max_media_bytes), 0)
        server_limit = await self._resolve_server_upload_limit_bytes()
        if server_limit is None:
            return local_limit
        return min(local_limit, server_limit) if local_limit else 0

    async def _upload_and_send_attachment(
        self, room_id: str, path: Path, limit_bytes: int,
        relates_to: dict[str, Any] | None = None,
    ) -> str | None:
        """Upload one local file to Matrix and send it as a media message. Returns failure marker or None."""
        if not self.client:
            return _ATTACH_UPLOAD_FAILED.format(path.name or _DEFAULT_ATTACH_NAME)

        resolved = path.expanduser().resolve(strict=False)
        filename = safe_filename(resolved.name) or _DEFAULT_ATTACH_NAME
        fail = _ATTACH_UPLOAD_FAILED.format(filename)

        if not resolved.is_file() or not self._is_workspace_path_allowed(resolved):
            return fail
        try:
            size_bytes = resolved.stat().st_size
        except OSError:
            return fail
        if limit_bytes <= 0 or size_bytes > limit_bytes:
            return _ATTACH_TOO_LARGE.format(filename)

        mime = mimetypes.guess_type(filename, strict=False)[0] or "application/octet-stream"
        try:
            with resolved.open("rb") as f:
                upload_result = await self.client.upload(
                    f, content_type=mime, filename=filename,
                    encrypt=self.config.e2ee_enabled and self._is_encrypted_room(room_id),
                    filesize=size_bytes,
                )
        except Exception:
            return fail

        upload_response = upload_result[0] if isinstance(upload_result, tuple) else upload_result
        encryption_info = upload_result[1] if isinstance(upload_result, tuple) and isinstance(upload_result[1], dict) else None
        if isinstance(upload_response, UploadError):
            return fail
        mxc_url = getattr(upload_response, "content_uri", None)
        if not isinstance(mxc_url, str) or not mxc_url.startswith("mxc://"):
            return fail

        content = self._build_outbound_attachment_content(
            filename=filename, mime=mime, size_bytes=size_bytes,
            mxc_url=mxc_url, encryption_info=encryption_info,
        )
        if relates_to:
            content["m.relates_to"] = relates_to
        try:
            await self._send_room_content(room_id, content)
        except Exception:
            return fail
        return None

    async def send(self, msg: OutboundMessage) -> None:
        """Send outbound content with streaming support via message edits."""
        if not self.client:
            return
        text = msg.content or ""
        candidates = self._collect_outbound_media_candidates(msg.media)
        relates_to = self._build_thread_relates_to(msg.metadata)
        is_progress = bool((msg.metadata or {}).get("_progress"))
        room_id = msg.chat_id

        # --- Streaming progress: edit a single message in-place ---
        if is_progress:
            session = self._streaming_sessions.get(room_id)
            if session is None:
                # First progress message: send new message and track its event_id
                content = _build_matrix_text_content(text)
                if relates_to:
                    content["m.relates_to"] = relates_to
                event_id = await self._send_room_content(room_id, content)
                if event_id:
                    self._streaming_sessions[room_id] = {
                        "event_id": event_id,
                        "last_content": text,
                        "last_edit_time": time.monotonic(),
                        "pending_content": None,
                        "flush_task": None,
                    }
                return
            # Subsequent progress: edit existing message (with throttling)
            if text == session["last_content"]:
                return
            now = time.monotonic()
            if now - session["last_edit_time"] >= STREAMING_EDIT_MIN_INTERVAL_S:
                try:
                    await self._send_room_content(
                        room_id, self._build_edit_content(session["event_id"], text),
                    )
                    session["last_content"] = text
                    session["last_edit_time"] = now
                    session["pending_content"] = None
                except Exception:
                    pass
            else:
                # Throttled — schedule a delayed flush so user still sees latest progress
                session["pending_content"] = text
                if not session.get("flush_task") or session["flush_task"].done():
                    session["flush_task"] = asyncio.create_task(
                        self._flush_streaming_edit(room_id),
                    )
            return

        # --- Final or non-progress message ---
        try:
            failures: list[str] = []
            if candidates:
                limit_bytes = await self._effective_media_limit_bytes()
                for path in candidates:
                    if fail := await self._upload_and_send_attachment(
                        room_id, path, limit_bytes, relates_to):
                        failures.append(fail)
            if failures:
                text = f"{text.rstrip()}\n{chr(10).join(failures)}" if text.strip() else "\n".join(failures)

            session = self._streaming_sessions.pop(room_id, None)
            if session:
                # Cancel any pending flush task
                if task := session.get("flush_task"):
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                # Final edit of the streaming message with complete response
                if text:
                    try:
                        await self._send_room_content(
                            room_id, self._build_edit_content(session["event_id"], text),
                        )
                    except Exception:
                        # Fallback: send as new message if edit fails
                        content = _build_matrix_text_content(text)
                        if relates_to:
                            content["m.relates_to"] = relates_to
                        await self._send_room_content(room_id, content)
            elif text or not candidates:
                content = _build_matrix_text_content(text)
                if relates_to:
                    content["m.relates_to"] = relates_to
                await self._send_room_content(room_id, content)
        finally:
            # Clean up any lingering session (e.g. on error path)
            if room_id in self._streaming_sessions:
                s = self._streaming_sessions.pop(room_id)
                if task := s.get("flush_task"):
                    task.cancel()
            await self._stop_typing_keepalive(room_id, clear_typing=True)

    def _register_event_callbacks(self) -> None:
        self.client.add_event_callback(self._on_message, RoomMessageText)
        self.client.add_event_callback(self._on_media_message, MATRIX_MEDIA_EVENT_FILTER)
        self.client.add_event_callback(self._on_room_invite, InviteEvent)

    def _register_response_callbacks(self) -> None:
        self.client.add_response_callback(self._on_sync_error, SyncError)
        self.client.add_response_callback(self._on_join_error, JoinError)
        self.client.add_response_callback(self._on_send_error, RoomSendError)

    def _log_response_error(self, label: str, response: Any) -> None:
        """Log Matrix response errors — auth errors at ERROR level, rest at WARNING."""
        code = getattr(response, "status_code", None)
        is_auth = code in {"M_UNKNOWN_TOKEN", "M_FORBIDDEN", "M_UNAUTHORIZED"}
        is_fatal = is_auth or getattr(response, "soft_logout", False)
        (logger.error if is_fatal else logger.warning)("Matrix {} failed: {}", label, response)

    async def _on_sync_error(self, response: SyncError) -> None:
        self._log_response_error("sync", response)

    async def _on_join_error(self, response: JoinError) -> None:
        self._log_response_error("join", response)

    async def _on_send_error(self, response: RoomSendError) -> None:
        self._log_response_error("send", response)

    async def _set_typing(self, room_id: str, typing: bool) -> None:
        """Best-effort typing indicator update."""
        if not self.client:
            return
        try:
            response = await self.client.room_typing(room_id=room_id, typing_state=typing,
                                                     timeout=TYPING_NOTICE_TIMEOUT_MS)
            if isinstance(response, RoomTypingError):
                logger.debug("Matrix typing failed for {}: {}", room_id, response)
        except Exception:
            pass

    async def _start_typing_keepalive(self, room_id: str) -> None:
        """Start periodic typing refresh (spec-recommended keepalive)."""
        await self._stop_typing_keepalive(room_id, clear_typing=False)
        await self._set_typing(room_id, True)
        if not self._running:
            return

        async def loop() -> None:
            try:
                while self._running:
                    await asyncio.sleep(TYPING_KEEPALIVE_INTERVAL_MS / 1000)
                    await self._set_typing(room_id, True)
            except asyncio.CancelledError:
                pass

        self._typing_tasks[room_id] = asyncio.create_task(loop())

    async def _stop_typing_keepalive(self, room_id: str, *, clear_typing: bool) -> None:
        if task := self._typing_tasks.pop(room_id, None):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if clear_typing:
            await self._set_typing(room_id, False)

    async def _sync_loop(self) -> None:
        while self._running:
            try:
                await self.client.sync_forever(timeout=30000, full_state=True)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(2)

    async def _on_room_invite(self, room: MatrixRoom, event: InviteEvent) -> None:
        allow_from = self.config.allow_from or []
        if not allow_from or event.sender in allow_from:
            await self.client.join(room.room_id)

    def _is_direct_room(self, room: MatrixRoom) -> bool:
        count = getattr(room, "member_count", None)
        return isinstance(count, int) and count <= 2

    def _is_bot_mentioned(self, event: RoomMessage) -> bool:
        """Check m.mentions payload AND message body for bot mention."""
        # 1. Check structured m.mentions (modern clients)
        source = getattr(event, "source", None)
        if isinstance(source, dict):
            mentions = (source.get("content") or {}).get("m.mentions")
            if isinstance(mentions, dict):
                user_ids = mentions.get("user_ids")
                if isinstance(user_ids, list) and self.config.user_id in user_ids:
                    return True
                if self.config.allow_room_mentions and mentions.get("room") is True:
                    return True
        # 2. Fallback: check message body text for @user_id (legacy clients)
        body = getattr(event, "body", None)
        if isinstance(body, str) and self.config.user_id:
            # Match both full mxid "@bot:server" and localpart "@bot"
            if self.config.user_id in body:
                return True
            localpart = self.config.user_id.split(":")[0]  # "@bot"
            if localpart and localpart in body:
                return True
        return False

    def _strip_mention_prefix(self, text: str) -> str:
        """Remove leading @bot mention from message body for cleaner context."""
        if not self.config.user_id:
            return text
        stripped = text.strip()
        # Try full mxid first, then localpart
        for mention in (self.config.user_id, self.config.user_id.split(":")[0]):
            if stripped.startswith(mention):
                stripped = stripped[len(mention):].lstrip(":,\u00a0 ")
                break
        return stripped or text

    def _should_process_message(self, room: MatrixRoom, event: RoomMessage) -> bool:
        """Apply sender and room policy checks."""
        if not self.is_allowed(event.sender):
            return False
        if self._is_direct_room(room):
            return True
        policy = self.config.group_policy
        if policy == "open":
            return True
        if policy == "allowlist":
            return room.room_id in (self.config.group_allow_from or [])
        if policy == "mention":
            return self._is_bot_mentioned(event)
        return False

    def _media_dir(self) -> Path:
        d = get_data_dir() / "media" / "matrix"
        d.mkdir(parents=True, exist_ok=True)
        return d

    @staticmethod
    def _event_source_content(event: RoomMessage) -> dict[str, Any]:
        source = getattr(event, "source", None)
        if not isinstance(source, dict):
            return {}
        content = source.get("content")
        return content if isinstance(content, dict) else {}

    def _event_thread_root_id(self, event: RoomMessage) -> str | None:
        relates_to = self._event_source_content(event).get("m.relates_to")
        if not isinstance(relates_to, dict) or relates_to.get("rel_type") != "m.thread":
            return None
        root_id = relates_to.get("event_id")
        return root_id if isinstance(root_id, str) and root_id else None

    def _thread_metadata(self, event: RoomMessage) -> dict[str, str] | None:
        if not (root_id := self._event_thread_root_id(event)):
            return None
        meta: dict[str, str] = {"thread_root_event_id": root_id}
        if isinstance(reply_to := getattr(event, "event_id", None), str) and reply_to:
            meta["thread_reply_to_event_id"] = reply_to
        return meta

    @staticmethod
    def _build_thread_relates_to(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
        if not metadata:
            return None
        root_id = metadata.get("thread_root_event_id")
        if not isinstance(root_id, str) or not root_id:
            return None
        reply_to = metadata.get("thread_reply_to_event_id") or metadata.get("event_id")
        if not isinstance(reply_to, str) or not reply_to:
            return None
        return {"rel_type": "m.thread", "event_id": root_id,
                "m.in_reply_to": {"event_id": reply_to}, "is_falling_back": True}

    def _event_attachment_type(self, event: MatrixMediaEvent) -> str:
        msgtype = self._event_source_content(event).get("msgtype")
        return _MSGTYPE_MAP.get(msgtype, "file")

    @staticmethod
    def _is_encrypted_media_event(event: MatrixMediaEvent) -> bool:
        return (isinstance(getattr(event, "key", None), dict)
                and isinstance(getattr(event, "hashes", None), dict)
                and isinstance(getattr(event, "iv", None), str))

    def _event_declared_size_bytes(self, event: MatrixMediaEvent) -> int | None:
        info = self._event_source_content(event).get("info")
        size = info.get("size") if isinstance(info, dict) else None
        return size if isinstance(size, int) and size >= 0 else None

    def _event_mime(self, event: MatrixMediaEvent) -> str | None:
        info = self._event_source_content(event).get("info")
        if isinstance(info, dict) and isinstance(m := info.get("mimetype"), str) and m:
            return m
        m = getattr(event, "mimetype", None)
        return m if isinstance(m, str) and m else None

    def _event_filename(self, event: MatrixMediaEvent, attachment_type: str) -> str:
        body = getattr(event, "body", None)
        if isinstance(body, str) and body.strip():
            if candidate := safe_filename(Path(body).name):
                return candidate
        return _DEFAULT_ATTACH_NAME if attachment_type == "file" else attachment_type

    def _build_attachment_path(self, event: MatrixMediaEvent, attachment_type: str,
                               filename: str, mime: str | None) -> Path:
        safe_name = safe_filename(Path(filename).name) or _DEFAULT_ATTACH_NAME
        suffix = Path(safe_name).suffix
        if not suffix and mime:
            if guessed := mimetypes.guess_extension(mime, strict=False):
                safe_name, suffix = f"{safe_name}{guessed}", guessed
        stem = (Path(safe_name).stem or attachment_type)[:72]
        suffix = suffix[:16]
        event_id = safe_filename(str(getattr(event, "event_id", "") or "evt").lstrip("$"))
        event_prefix = (event_id[:24] or "evt").strip("_")
        return self._media_dir() / f"{event_prefix}_{stem}{suffix}"

    async def _download_media_bytes(self, mxc_url: str) -> bytes | None:
        if not self.client:
            return None
        response = await self.client.download(mxc=mxc_url)
        if isinstance(response, DownloadError):
            logger.warning("Matrix download failed for {}: {}", mxc_url, response)
            return None
        body = getattr(response, "body", None)
        if isinstance(body, (bytes, bytearray)):
            return bytes(body)
        if isinstance(response, MemoryDownloadResponse):
            return bytes(response.body)
        if isinstance(body, (str, Path)):
            path = Path(body)
            if path.is_file():
                try:
                    return path.read_bytes()
                except OSError:
                    return None
        return None

    def _decrypt_media_bytes(self, event: MatrixMediaEvent, ciphertext: bytes) -> bytes | None:
        key_obj, hashes, iv = getattr(event, "key", None), getattr(event, "hashes", None), getattr(event, "iv", None)
        key = key_obj.get("k") if isinstance(key_obj, dict) else None
        sha256 = hashes.get("sha256") if isinstance(hashes, dict) else None
        if not all(isinstance(v, str) for v in (key, sha256, iv)):
            return None
        try:
            return decrypt_attachment(ciphertext, key, sha256, iv)
        except (EncryptionError, ValueError, TypeError):
            logger.warning("Matrix decrypt failed for event {}", getattr(event, "event_id", ""))
            return None

    async def _fetch_media_attachment(
        self, room: MatrixRoom, event: MatrixMediaEvent,
    ) -> tuple[dict[str, Any] | None, str]:
        """Download, decrypt if needed, and persist a Matrix attachment."""
        atype = self._event_attachment_type(event)
        mime = self._event_mime(event)
        filename = self._event_filename(event, atype)
        mxc_url = getattr(event, "url", None)
        fail = _ATTACH_FAILED.format(filename)

        if not isinstance(mxc_url, str) or not mxc_url.startswith("mxc://"):
            return None, fail

        limit_bytes = await self._effective_media_limit_bytes()
        declared = self._event_declared_size_bytes(event)
        if declared is not None and declared > limit_bytes:
            return None, _ATTACH_TOO_LARGE.format(filename)

        downloaded = await self._download_media_bytes(mxc_url)
        if downloaded is None:
            return None, fail

        encrypted = self._is_encrypted_media_event(event)
        data = downloaded
        if encrypted:
            if (data := self._decrypt_media_bytes(event, downloaded)) is None:
                return None, fail

        if len(data) > limit_bytes:
            return None, _ATTACH_TOO_LARGE.format(filename)

        path = self._build_attachment_path(event, atype, filename, mime)
        try:
            path.write_bytes(data)
        except OSError:
            return None, fail

        attachment = {
            "type": atype, "mime": mime, "filename": filename,
            "event_id": str(getattr(event, "event_id", "") or ""),
            "encrypted": encrypted, "size_bytes": len(data),
            "path": str(path), "mxc_url": mxc_url,
        }
        return attachment, _ATTACH_MARKER.format(path)

    def _resolve_sender_name(self, room: MatrixRoom, sender_id: str) -> str:
        """Get human-readable display name for a sender in a room."""
        try:
            name = room.user_name(sender_id)
            if name:
                return name
        except Exception:
            pass
        # Fallback: extract localpart from @user:server
        if sender_id.startswith("@") and ":" in sender_id:
            return sender_id[1:].split(":")[0]
        return sender_id

    def _base_metadata(self, room: MatrixRoom, event: RoomMessage) -> dict[str, Any]:
        """Build common metadata for text and media handlers."""
        meta: dict[str, Any] = {"room": getattr(room, "display_name", room.room_id)}
        sender_name = self._resolve_sender_name(room, event.sender)
        meta["sender_name"] = sender_name
        if isinstance(eid := getattr(event, "event_id", None), str) and eid:
            meta["event_id"] = eid
        if thread := self._thread_metadata(event):
            meta.update(thread)
        return meta

    async def _on_message(self, room: MatrixRoom, event: RoomMessageText) -> None:
        if event.sender == self.config.user_id or not self._should_process_message(room, event):
            return
        await self._start_typing_keepalive(room.room_id)
        try:
            meta = self._base_metadata(room, event)
            content = event.body
            is_group = not self._is_direct_room(room)
            if is_group:
                content = self._strip_mention_prefix(content)
                sender_name = meta.get("sender_name", event.sender)
                room_name = meta.get("room", room.room_id)
                content = f"[群聊: {room_name}] {sender_name}: {content}"
            await self._handle_message(
                sender_id=event.sender, chat_id=room.room_id,
                content=content, metadata=meta,
            )
        except Exception:
            await self._stop_typing_keepalive(room.room_id, clear_typing=True)
            raise

    async def _on_media_message(self, room: MatrixRoom, event: MatrixMediaEvent) -> None:
        if event.sender == self.config.user_id or not self._should_process_message(room, event):
            return
        attachment, marker = await self._fetch_media_attachment(room, event)
        parts: list[str] = []
        if isinstance(body := getattr(event, "body", None), str) and body.strip():
            parts.append(body.strip())
        parts.append(marker)

        await self._start_typing_keepalive(room.room_id)
        try:
            meta = self._base_metadata(room, event)
            if attachment:
                meta["attachments"] = [attachment]
            content = "\n".join(parts)
            is_group = not self._is_direct_room(room)
            if is_group:
                sender_name = meta.get("sender_name", event.sender)
                room_name = meta.get("room", room.room_id)
                content = f"[群聊: {room_name}] {sender_name}: {content}"
            await self._handle_message(
                sender_id=event.sender, chat_id=room.room_id,
                content=content,
                media=[attachment["path"]] if attachment else [],
                metadata=meta,
            )
        except Exception:
            await self._stop_typing_keepalive(room.room_id, clear_typing=True)
            raise
