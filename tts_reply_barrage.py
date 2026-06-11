"""
纯音频直播弹幕回复系统（LLM版）
弹幕 → 豆包LLM流式回复 → 流式TTS播报
独立于 tts_broadcast.py，仅复用其 TTS 引擎
"""

import asyncio
import json
import logging
import time
import uuid
from typing import AsyncIterator

import httpx

import config
from main import (
    TTSEngine, RealTimePCMPlayer,
    MsgType, EventType,
    _build_start_session, _build_task_request, _build_finish_session,
    _receive_message, _wait_for_event,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ====== 频率状态 ======
last_reply_time = 0
reply_count_in_minute = 0
minute_start_time = 0
replied_contents = set()


def should_reply(user_input: str) -> bool:
    """频率控制：每分钟上限、间隔、去重"""
    global last_reply_time, reply_count_in_minute, minute_start_time
    now = time.time()
    if now - minute_start_time > 60:
        minute_start_time = now
        reply_count_in_minute = 0
        replied_contents.clear()
    if reply_count_in_minute >= config.danmu_max_replies_per_minute:
        return False
    if now - last_reply_time < config.danmu_min_reply_interval:
        return False
    key = user_input.strip().lower()
    if key in replied_contents:
        return False
    return True


# ====== 商品知识 ======

def build_system_prompt() -> str:
    """从 answer.txt 构建商品知识 Prompt"""
    prompt = (
        '你是抖音卖女鞋的直播间主播，回复顾客弹幕。\n'
        '回复要求：\n'
        '- 称呼用"姐妹们""宝子"，语气年轻利落，每句结尾加~\n'
        '- 回复1~2句话，控制在40字以内\n'
        '- 模仿【参考话术】里的风格和用词\n'
        '- 根据【商品信息】回答事实，不要乱编\n\n'
        '参考话术 & 商品信息：\n'
    )
    try:
        with open(config.product_info_file, "r", encoding="utf-8") as f:
            prompt += f.read().strip()
    except FileNotFoundError:
        prompt += "(暂无)"
    return prompt


# ====== LLM 流式调用 ======

async def llm_stream_chars(messages: list) -> AsyncIterator[str]:
    """豆包流式API，逐 token yield"""
    async with httpx.AsyncClient(timeout=30) as client:
        async with client.stream(
            "POST",
            f"{config.llm_base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {config.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.llm_model,
                "stream": True,
                "thinking": {"type": "disabled"},
                "messages": messages,
            },
        ) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data = line[6:]
                    if data.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            yield content
                    except json.JSONDecodeError:
                        pass


# ====== 扩展 TTS：支持字符流输入 ======

class BarrageTTSEngine(TTSEngine):
    """在 TTSEngine 基础上增加字符流合成"""

    async def synthesize_char_stream(self, char_stream: AsyncIterator[str]) -> AsyncIterator[bytes]:
        """接收字符流，流式合成，逐块 yield 音频"""
        session_id = str(uuid.uuid4())
        await self.ws.send(_build_start_session(
            session_id, config.tts_speaker_id, config.tts_format, config.tts_sample_rate
        ).marshal())
        await _wait_for_event(self.ws, MsgType.FullServerResponse, EventType.SessionStarted)

        async def send_chars():
            async for char in char_stream:
                await self.ws.send(_build_task_request(
                    session_id, char,
                    config.tts_speaker_id, config.tts_format, config.tts_sample_rate
                ).marshal())
                await asyncio.sleep(0.0045)
            await self.ws.send(_build_finish_session(session_id).marshal())

        send_task = asyncio.create_task(send_chars())
        try:
            while True:
                msg = await _receive_message(self.ws)
                if msg.type == MsgType.FullServerResponse and msg.event == EventType.SessionFinished:
                    break
                elif msg.type == MsgType.AudioOnlyServer and msg.payload:
                    yield msg.payload
        finally:
            await send_task


# ====== 主逻辑 ======

async def speak_reply(tts: BarrageTTSEngine, player: RealTimePCMPlayer, user_input: str):
    """LLM生成回复 → 流式TTS → 播放"""
    system_prompt = build_system_prompt()
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_input},
    ]

    logger.info(f"📝 System Prompt:\n{system_prompt}")
    logger.info(f"🤖 LLM 请求：{user_input}")

    # 包一层打印每个 token
    async def log_stream():
        reply_text = ""
        async for token in llm_stream_chars(messages):
            reply_text += token
            logger.info(f"🤖 LLM token: {token}")
            yield token
        logger.info(f"📝 LLM 完整回复: {reply_text}")

    async for audio_chunk in tts.synthesize_char_stream(log_stream()):
        player.play_chunk(audio_chunk)
    player.flush()
    logger.info(f"✅ 回复播报完成")


async def process_input(tts: BarrageTTSEngine, player: RealTimePCMPlayer, user_input: str):
    """处理单条弹幕"""
    global last_reply_time, reply_count_in_minute

    if not user_input.strip():
        return

    if not should_reply(user_input):
        print("⏳ 频率限制，跳过")
        return

    last_reply_time = time.time()
    reply_count_in_minute += 1
    replied_contents.add(user_input.strip().lower())

    await speak_reply(tts, player, user_input)


async def main():
    tts = BarrageTTSEngine()
    player = RealTimePCMPlayer()

    try:
        await tts.connect()
        logger.info("✅ TTS 连接成功")

        print("\n===== 弹幕模拟器（LLM版） =====")
        print("输入问题后回车，按 q 退出\n")

        loop = asyncio.get_running_loop()
        while True:
            user_input = await loop.run_in_executor(None, input, ">> ")
            if user_input.strip().lower() in ("q", "quit", "exit"):
                break
            await process_input(tts, player, user_input)

    except KeyboardInterrupt:
        pass
    finally:
        await tts.disconnect()
        player.close()
        print("已退出")


if __name__ == "__main__":
    asyncio.run(main())
