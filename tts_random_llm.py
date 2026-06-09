"""
可插拔的口播 LLM 内容生成器（流式版）
LLM token 逐字 → TTS 逐字合成 → 逐块播报，零等待
不存在此文件时跳过，零影响
"""

import asyncio
import json
import logging
import uuid

import httpx
import config

logger = logging.getLogger(__name__)

# ====== Prompt ======

SYSTEM_PROMPT = (
    "你是卖女鞋的直播间主播，正在口播带货。"
    "根据对话历史和商品信息，自然地补一句，不要重复原话，控制在25字内~\n\n"
    "参考话术风格：\n"
    "- 姐妹们看这款爆款凉拖！\n"
    "- 专柜一百多，今天直播间只要39.9！\n"
    "- 源头工厂直发没有中间商~\n"
    "- 下雨天穿都稳得很，姐妹们放心穿~\n"
)


async def generate_and_play_interject(tts, player, prev_sentence: str):
    """LLM流式 → TTS逐字 → 逐块播报，全程不等"""
    # 延迟导入避免循环依赖
    from tts_broadcast import (
        _build_start_session, _build_task_request, _build_finish_session,
        _wait_for_event, _receive_message, MsgType, EventType,
    )

    # 1. 打开 TTS session
    session_id = str(uuid.uuid4())
    await tts.ws.send(_build_start_session(
        session_id, config.tts_speaker_id, config.tts_format, config.tts_sample_rate
    ).marshal())
    await _wait_for_event(tts.ws, MsgType.FullServerResponse, EventType.SessionStarted)

    # 2. 启动 LLM 流式请求
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"刚说完：{prev_sentence}\n顺着补一句 要求:语气语言自然过度："},
    ]

    # 3. 并发任务：LLM吐字符 → 送TTS
    async def send_llm_to_tts():
        async with httpx.AsyncClient(timeout=15) as client:
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
                    "max_tokens": 60,
                },
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data = line[6:]
                        if data.strip() == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data)
                            content = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
                            if not content:
                                continue
                            # 每个字符送 TTS
                            for c in content:
                                await tts.ws.send(_build_task_request(
                                    session_id, c,
                                    config.tts_speaker_id, config.tts_format, config.tts_sample_rate
                                ).marshal())
                                await asyncio.sleep(0.0045)
                        except json.JSONDecodeError:
                            pass
                await tts.ws.send(_build_finish_session(session_id).marshal())

    # 4. 并发接收：TTS音频块 → 播放
    send_task = asyncio.ensure_future(send_llm_to_tts())
    reply_text = ""
    try:
        while True:
            msg = await _receive_message(tts.ws)
            if msg.type == MsgType.FullServerResponse and msg.event == EventType.SessionFinished:
                break
            elif msg.type == MsgType.AudioOnlyServer and msg.payload:
                player.play_chunk(msg.payload)
    finally:
        await send_task
    player.flush()
