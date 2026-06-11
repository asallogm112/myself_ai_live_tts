"""
可插拔的口播 LLM 内容生成器（流式版）
LLM token 逐字 → TTS 逐字合成 → 逐块播报，零等待
不存在此文件时跳过，零影响
"""

import asyncio
import json
import logging
import time
import uuid

import httpx
import config

logger = logging.getLogger(__name__)

# ====== Prompt（从 config.py 读取，避免缩进问题） ======


async def generate_and_play_interject(tts, player, prev_sentence: str):
    """LLM流式 → TTS逐字 → 逐块播报，全程不等"""
    # 延迟导入避免循环依赖
    from main import (
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
        {"role": "system", "content": config.llm_content_system_prompt},
        {"role": "user", "content": f"刚说完：{prev_sentence}\n顺着补一句 要求:语气语言自然过度："},
    ]

    # 3. 并发任务：LLM吐字符 → 送TTS
    llm_start_time = time.time()
    logger.info(f"⏱️ LLM 请求开始")
    first_audio_logged = False

    async def send_llm_to_tts():
        llm_reply = ""
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
                            llm_reply += content
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
        if llm_reply:
            logger.info(f"🤖 LLM 生成: {llm_reply}")

    # 4. 并发接收：TTS音频块 → 播放
    send_task = asyncio.ensure_future(send_llm_to_tts())
    reply_text = ""
    try:
        while True:
            msg = await _receive_message(tts.ws)
            if msg.type == MsgType.FullServerResponse and msg.event == EventType.SessionFinished:
                break
            elif msg.type == MsgType.AudioOnlyServer and msg.payload:
                if not first_audio_logged:
                    first_audio_logged = True
                    elapsed = time.time() - llm_start_time
                    logger.info(f"⏱️ 首音到达：{elapsed:.2f}s")
                player.play_chunk(msg.payload)
    finally:
        await send_task
    player.flush()
