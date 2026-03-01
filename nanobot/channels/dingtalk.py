import asyncio
import json
import mimetypes
import os
import time
import uuid
import random
from typing import Any, Dict, Set
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import DingTalkConfig

try:
    from dingtalk_stream import (
        DingTalkStreamClient,
        Credential,
        CallbackHandler,
        CallbackMessage,
        AckMessage,
        AICardReplier,
    )
    from dingtalk_stream.chatbot import ChatbotMessage
    DINGTALK_AVAILABLE = True
except ImportError:
    DINGTALK_AVAILABLE = False
    CallbackHandler = object

class NanobotDingTalkHandler(CallbackHandler):
    """负责接收消息并初始化流式会话"""
    def __init__(self, channel: "DingTalkChannel"):
        super().__init__()
        self.channel = channel

    async def process(self, message: CallbackMessage):
        try:
            chatbot_msg = ChatbotMessage.from_dict(message.data)
            sender_id = chatbot_msg.sender_staff_id or chatbot_msg.sender_id
            sender_name = chatbot_msg.sender_nick or "Unknown"
            
            content = ""
            if chatbot_msg.text:
                content = chatbot_msg.text.content.strip()

            if not content:
                return AckMessage.STATUS_OK, "OK"

            logger.info(f"DingTalk Inbound: {sender_name} ({sender_id}) -> {content}")

            # 1. 立即创建 AI 卡片回复器
            card_template_id = "9a7c7203-1552-4ff0-8ede-ee6224dbce20.schema"
            replier = AICardReplier(self.channel._client, chatbot_msg)
            
            # 2. 投放初始卡片（思考中）
            card_instance_id = replier.create_and_send_card(
                card_template_id,
                card_data={"content": "🤔 正在思考中..."},
                callback_type="STREAM",
                at_sender=True
            )

            # 3. 初始化本地会话追踪
            self.channel.init_session(sender_id, replier, card_instance_id)

            # 4. 启动 Nanobot 异步任务
            task = asyncio.create_task(
                self.channel._on_message(content, sender_id, sender_name)
            )
            self.channel._background_tasks.add(task)
            task.add_done_callback(self.channel._background_tasks.discard)

            return AckMessage.STATUS_OK, "OK"
        except Exception as e:
            logger.error(f"Handler Error: {e}")
            return AckMessage.STATUS_OK, "Error"

class DingTalkChannel(BaseChannel):
    name = "dingtalk"
    SESSION_TIMEOUT = 300  # 5分钟超时，应对耗时工具
    _IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
    _AUDIO_EXTS = {".amr", ".mp3", ".wav", ".ogg", ".m4a", ".aac"}
    _VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}

    def __init__(self, config: DingTalkConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config = config
        self._client = None
        self._active_sessions: Dict[str, Dict[str, Any]] = {}
        self._background_tasks: Set[asyncio.Task] = set()
        # 新增 http 客户端和 token 管理
        try:
            import httpx
            self._http = httpx.AsyncClient()
        except ImportError:
            self._http = None
        self._access_token: str | None = None
        self._token_expiry: float = 0

    def init_session(self, chat_id: str, replier: Any, instance_id: str):
        """初始化流式会话容器"""
        watchdog = asyncio.create_task(self._session_watchdog(chat_id))
        self._background_tasks.add(watchdog)
        
        self._active_sessions[chat_id] = {
            "replier": replier,
            "instance_id": instance_id,
            "full_content": "",
            "is_finished": False,
            "lock": asyncio.Lock(),
            "last_sent_content": "",
            "watchdog_task": watchdog,
            "last_activity": time.monotonic(),
        }

    def _create_proactive_card(self, chat_id: str):
        """在线程池中创建卡片，返回 (replier, card_instance_id) 或 None"""
        try:
            # 构造虚拟 ChatbotMessage，模拟单聊场景
            fake_msg = ChatbotMessage()
            fake_msg.sender_id = chat_id
            fake_msg.sender_staff_id = chat_id
            fake_msg.sender_corp_id = ""
            fake_msg.conversation_id = ""
            fake_msg.message_id = str(uuid.uuid4())
            fake_msg.conversation_type = "1"  # 单聊
            fake_msg.sender_nick = ""
            fake_msg.hosting_context = None

            card_template_id = "9a7c7203-1552-4ff0-8ede-ee6224dbce20.schema"
            replier = AICardReplier(self._client, fake_msg)

            card_instance_id = replier.create_and_send_card(
                card_template_id,
                card_data={"content": "🤔 正在思考中..."},
                callback_type="STREAM",
                at_sender=False,
            )

            if not card_instance_id:
                logger.error(f"Proactive card creation returned empty instance_id for {chat_id}")
                return None

            logger.info(f"Proactive card created for {chat_id}")
            return replier, card_instance_id
        except Exception as e:
            logger.error(f"Failed to create proactive card: {e}")
            return None

    async def _session_watchdog(self, chat_id: str):
        """防止会话死挂"""
        try:
            while chat_id in self._active_sessions:
                await asyncio.sleep(10)
                session = self._active_sessions.get(chat_id)
                if not session or session["is_finished"]: break
                if time.monotonic() - session["last_activity"] > self.SESSION_TIMEOUT:
                    logger.warning(f"Session {chat_id} timeout.")
                    await self.finalize_session(chat_id, "timeout")
                    break
        except asyncio.CancelledError: pass

    @staticmethod
    def _is_http_url(value: str) -> bool:
        return urlparse(value).scheme in ("http", "https")

    def _guess_upload_type(self, media_ref: str) -> str:
        ext = Path(urlparse(media_ref).path).suffix.lower()
        if ext in self._IMAGE_EXTS: return "image"
        if ext in self._AUDIO_EXTS: return "voice"
        if ext in self._VIDEO_EXTS: return "video"
        return "file"

    def _guess_filename(self, media_ref: str, upload_type: str) -> str:
        name = os.path.basename(urlparse(media_ref).path)
        return name or {"image": "image.jpg", "voice": "audio.amr", "video": "video.mp4"}.get(upload_type, "file.bin")

    async def _read_media_bytes(
        self,
        media_ref: str,
    ) -> tuple[bytes | None, str | None, str | None]:
        if not media_ref:
            return None, None, None

        if self._is_http_url(media_ref):
            if not self._http:
                return None, None, None
            try:
                resp = await self._http.get(media_ref, follow_redirects=True)
                if resp.status_code >= 400:
                    logger.warning(
                        "DingTalk media download failed status={} ref={}",
                        resp.status_code,
                        media_ref,
                    )
                    return None, None, None
                content_type = (resp.headers.get("content-type") or "").split(";")[0].strip()
                filename = self._guess_filename(media_ref, self._guess_upload_type(media_ref))
                return resp.content, filename, content_type or None
            except Exception as e:
                logger.error("DingTalk media download error ref={} err={}", media_ref, e)
                return None, None, None

        try:
            if media_ref.startswith("file://"):
                parsed = urlparse(media_ref)
                local_path = Path(unquote(parsed.path))
            else:
                local_path = Path(os.path.expanduser(media_ref))
            if not local_path.is_file():
                logger.warning("DingTalk media file not found: {}", local_path)
                return None, None, None
            data = await asyncio.to_thread(local_path.read_bytes)
            content_type = mimetypes.guess_type(local_path.name)[0]
            return data, local_path.name, content_type
        except Exception as e:
            logger.error("DingTalk media read error ref={} err={}", media_ref, e)
            return None, None, None

    async def _upload_media(
        self,
        token: str,
        data: bytes,
        media_type: str,
        filename: str,
        content_type: str | None,
    ) -> str | None:
        if not self._http:
            return None
        url = f"https://oapi.dingtalk.com/media/upload?access_token={token}&type={media_type}"
        mime = content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        files = {"media": (filename, data, mime)}

        try:
            resp = await self._http.post(url, files=files)
            text = resp.text
            result = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            if resp.status_code >= 400:
                logger.error("DingTalk media upload failed status={} type={} body={}", resp.status_code, media_type, text[:500])
                return None
            errcode = result.get("errcode", 0)
            if errcode != 0:
                logger.error("DingTalk media upload api error type={} errcode={} body={}", media_type, errcode, text[:500])
                return None
            sub = result.get("result") or {}
            media_id = result.get("media_id") or result.get("mediaId") or sub.get("media_id") or sub.get("mediaId")
            if not media_id:
                logger.error("DingTalk media upload missing media_id body={}", text[:500])
                return None
            return str(media_id)
        except Exception as e:
            logger.error("DingTalk media upload error type={} err={}", media_type, e)
            return None

    async def _get_access_token(self) -> str | None:
        """获取或刷新 Access Token"""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        data = {
            "appKey": self.config.client_id,
            "appSecret": self.config.client_secret,
        }

        if not self._http:
            logger.warning("DingTalk HTTP client not initialized, cannot refresh token")
            return None

        try:
            resp = await self._http.post(url, json=data)
            resp.raise_for_status()
            res_data = resp.json()
            self._access_token = res_data.get("accessToken")
            self._token_expiry = time.time() + int(res_data.get("expireIn", 7200)) - 60
            return self._access_token
        except Exception as e:
            logger.error("Failed to get DingTalk access token: {}", e)
            return None

    async def _send_batch_message(
        self,
        token: str,
        chat_id: str,
        msg_key: str,
        msg_param: dict[str, Any],
    ) -> bool:
        if not self._http:
            logger.warning("DingTalk HTTP client not initialized, cannot send")
            return False

        url = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
        headers = {"x-acs-dingtalk-access-token": token}
        payload = {
            "robotCode": self.config.client_id,
            "userIds": [chat_id],
            "msgKey": msg_key,
            "msgParam": json.dumps(msg_param, ensure_ascii=False),
        }

        try:
            resp = await self._http.post(url, json=payload, headers=headers)
            body = resp.text
            if resp.status_code != 200:
                logger.error("DingTalk send failed msgKey={} status={} body={}", msg_key, resp.status_code, body[:500])
                return False
            try:
                result = resp.json()
            except Exception:
                result = {}
            errcode = result.get("errcode")
            if errcode not in (None, 0):
                logger.error("DingTalk send api error msgKey={} errcode={} body={}", msg_key, errcode, body[:500])
                return False
            logger.debug("DingTalk message sent to {} with msgKey={}", chat_id, msg_key)
            return True
        except Exception as e:
            logger.error("Error sending DingTalk message msgKey={} err={}", msg_key, e)
            return False

    async def _send_markdown_text(self, token: str, chat_id: str, content: str) -> bool:
        return await self._send_batch_message(
            token,
            chat_id,
            "sampleMarkdown",
            {"text": content, "title": "Nanobot Reply"},
        )

    async def _send_media_ref(self, token: str, chat_id: str, media_ref: str) -> bool:
        media_ref = (media_ref or "").strip()
        if not media_ref:
            return True

        upload_type = self._guess_upload_type(media_ref)
        if upload_type == "image" and self._is_http_url(media_ref):
            ok = await self._send_batch_message(
                token,
                chat_id,
                "sampleImageMsg",
                {"photoURL": media_ref},
            )
            if ok:
                return True
            logger.warning("DingTalk image url send failed, trying upload fallback: {}", media_ref)

        data, filename, content_type = await self._read_media_bytes(media_ref)
        if not data:
            logger.error("DingTalk media read failed: {}", media_ref)
            return False

        filename = filename or self._guess_filename(media_ref, upload_type)
        file_type = Path(filename).suffix.lower().lstrip(".")
        if not file_type:
            guessed = mimetypes.guess_extension(content_type or "")
            file_type = (guessed or ".bin").lstrip(".")
        if file_type == "jpeg":
            file_type = "jpg"

        media_id = await self._upload_media(
            token=token,
            data=data,
            media_type=upload_type,
            filename=filename,
            content_type=content_type,
        )
        if not media_id:
            return False

        if upload_type == "image":
            # Verified in production: sampleImageMsg accepts media_id in photoURL.
            ok = await self._send_batch_message(
                token,
                chat_id,
                "sampleImageMsg",
                {"photoURL": media_id},
            )
            if ok:
                return True
            logger.warning("DingTalk image media_id send failed, falling back to file: {}", media_ref)

        return await self._send_batch_message(
            token,
            chat_id,
            "sampleFile",
            {"mediaId": media_id, "fileName": filename, "fileType": file_type},
        )

    async def send(self, msg: OutboundMessage) -> None:
        """发送消息：结合流式卡片更新与独立文件发送"""
        token = await self._get_access_token()
        if not token:
            logger.error("DingTalk access token 获取失败，消息未发送")
            return

        chat_id = msg.chat_id
        content = msg.content or ""
        is_final = msg.metadata.get("final") if msg.metadata else False
        loop = asyncio.get_running_loop()

        # --- 第一阶段：处理流式卡片 UI ---
        # 场景 A: 更新已存在的流式卡片 (用户正在互动)
        if chat_id in self._active_sessions:
            session = self._active_sessions[chat_id]
            if not session["is_finished"]:
                session["last_activity"] = time.monotonic()

                # 逻辑处理：增量累加或全量覆盖
                if len(content) < 5:
                    session["full_content"] += content
                else:
                    session["full_content"] = content
                
                logger.debug(f"Updating card: {chat_id}")
                async with session["lock"]:
                    if session["full_content"] != session["last_sent_content"] or is_final:
                        await loop.run_in_executor(None, lambda: session["replier"].streaming(
                            card_instance_id=session["instance_id"],
                            content_key="content",
                            content_value=session["full_content"],
                            append=False,
                            finished=is_final,
                            failed=False
                        ))
                        session["last_sent_content"] = session["full_content"]

                if is_final:
                    session["is_finished"] = True
                    self._cleanup_session(chat_id)

        # 场景 B: 没有活跃会话（例如机器人主动推送），先创建卡片占位
        elif content or msg.media:
            logger.info(f"No active session for {chat_id}, creating proactive card")
            result = await loop.run_in_executor(None, self._create_proactive_card, chat_id)
            if result:
                replier, card_instance_id = result
                self.init_session(chat_id, replier, card_instance_id)
                session = self._active_sessions[chat_id]
                session["full_content"] = content
                
                async with session["lock"]:
                    await loop.run_in_executor(None, lambda: session["replier"].streaming(
                        card_instance_id=session["instance_id"],
                        content_key="content",
                        content_value=session["full_content"],
                        append=False,
                        finished=is_final,
                        failed=False
                    ))
                if is_final:
                    session["is_finished"] = True
                    self._cleanup_session(chat_id)

        # --- 第二阶段：处理独立消息（文本与文件附件） ---
        # 注意：文件发送通常在任务结束(is_final)或者有明确媒体对象时触发
        
        # 1. 如果有媒体附件，执行上传并发送
        if msg.media:
            for media_ref in msg.media:
                logger.info(f"Processing media attachment: {media_ref}")
                ok = await self._send_media_ref(token, chat_id, media_ref)
                if not ok:
                    logger.error(f"DingTalk media send failed for {media_ref}")
                    filename = self._guess_filename(media_ref, self._guess_upload_type(media_ref))
                    await self._send_markdown_text(
                        token, 
                        chat_id, 
                        f"⚠️ 附件发送失败: {filename}"
                    )
    def _cleanup_session(self, chat_id: str):
        session = self._active_sessions.pop(chat_id, None)
        if session and session["watchdog_task"]:
            session["watchdog_task"].cancel()

    async def finalize_session(self, chat_id: str, reason: str):
        if chat_id in self._active_sessions:
            session = self._active_sessions[chat_id]
            final_text = session["full_content"] or f"任务已完成 ({reason})"
            await self._streaming_with_retry(session, final_text, finished=True)
            self._cleanup_session(chat_id)

    async def _on_message(self, content: str, sender_id: str, sender_name: str) -> None:
        await self._handle_message(
            sender_id=sender_id,
            chat_id=sender_id,
            content=f"{sender_name}从钉钉发来消息：{content}",
            metadata={"sender_name": sender_name, "platform": "dingtalk"},
        )

    async def start(self) -> None:
        if not DINGTALK_AVAILABLE: return
        self._running = True
        credential = Credential(self.config.client_id, self.config.client_secret)
        self._client = DingTalkStreamClient(credential)
        self._client.register_callback_handler(ChatbotMessage.TOPIC, NanobotDingTalkHandler(self))
        logger.info("DingTalk AI Card Channel (Hybrid Mode) Started.")
        while self._running:
            try:
                await self._client.start()
            except Exception as e:
                logger.warning(f"Retry in 5s: {e}")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        for t in self._background_tasks: t.cancel()