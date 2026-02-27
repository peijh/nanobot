import asyncio
import json
import time
import uuid
import random
from typing import Any, Dict, Set
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

    def __init__(self, config: DingTalkConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config = config
        self._client = None
        self._active_sessions: Dict[str, Dict[str, Any]] = {}
        self._background_tasks: Set[asyncio.Task] = set()

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

    async def send(self, msg: OutboundMessage) -> None:
        """核心发送方法：自动路由流式回复与主动推送"""
        chat_id = msg.chat_id
        content = msg.content or ""
        
        # --- 场景 1: 更新已存在的流式卡片 (用户正在互动) ---
        if chat_id in self._active_sessions:
            session = self._active_sessions[chat_id]
            if not session["is_finished"]:
                session["last_activity"] = time.monotonic()
                is_final = msg.metadata.get("final") if msg.metadata else False

                # 逻辑处理：如果是结果帧，覆盖思考过程；否则累加并处理换行
                if len(content) < 5 :
                    session["full_content"] += content
                else:
                    session["full_content"] = content
                logger.debug(session["full_content"])
                # 流式发送
                loop = asyncio.get_running_loop()
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
                return

        # --- 场景 2: 没有 active session，先创建 AI 卡片会话再发送 ---
        logger.info(f"No active session for {chat_id}, creating proactive card session")
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._create_proactive_card, chat_id)
        if result is None:
            logger.error(f"Card session creation failed for {chat_id}, message dropped")
            return

        replier, card_instance_id = result
        self.init_session(chat_id, replier, card_instance_id)
        session = self._active_sessions[chat_id]
        session["last_activity"] = time.monotonic()
        is_final = msg.metadata.get("final") if msg.metadata else False
        session["full_content"] = content
        logger.debug(content)

        async with session["lock"]:
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