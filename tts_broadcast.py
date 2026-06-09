import asyncio
import io
import re
import struct
import uuid
import logging
import os
import hashlib
import time
import random
from dataclasses import dataclass
from enum import IntEnum, Enum
from typing import AsyncIterator, List, Optional
from pathlib import Path

import websockets
import pyaudio

# ======================= 直接导入 config.py 配置（唯一改动点） =======================
import config

# 日志配置
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
# 屏蔽websockets无用警告
logging.getLogger("websockets").setLevel(logging.ERROR)

# ========== 缓存工具函数（仅文本+音色） ==========
def get_cache_key(text: str) -> str:
    """生成唯一缓存键：使用文案前10个字作为文件名，过滤非法字符"""
    # 1. 清理文本，取前10个字符
    clean_text = text.strip()[:10]
    # 2. 过滤文件名非法字符（\ / : * ? " < > |）
    invalid_chars = r'\/:*?"<>|'
    for char in invalid_chars:
        clean_text = clean_text.replace(char, "")
    # 3. 返回处理后的文件名（不加后缀，后缀在保存时添加）
    return clean_text

def save_to_cache(cache_key: str, audio_data: bytes):
    """保存音频到本地缓存"""
    file_path = os.path.join(config.broadcast_cache_dir, f"{cache_key}.pcm")
    try:
        with open(file_path, "wb") as f:
            f.write(audio_data)
        logger.debug(f"✅ 缓存已保存: {cache_key}")
    except Exception as e:
        logger.warning(f"⚠️ 缓存保存失败: {e}")

def load_from_cache(cache_key: str) -> Optional[bytes]:
    """从本地缓存读取音频，失败返回None"""
    file_path = os.path.join(config.broadcast_cache_dir, f"{cache_key}.pcm")
    if os.path.exists(file_path):
        try:
            with open(file_path, "rb") as f:
                return f.read()
        except Exception as e:
            logger.warning(f"⚠️ 缓存读取失败，将重新合成: {e}")
            # 损坏的缓存文件自动删除
            try:
                os.remove(file_path)
            except:
                pass
    return None


# ========== 二进制协议编解码（官方标准） ==========
class MsgType(IntEnum):
    Invalid = 0
    FullClientRequest = 0b1
    AudioOnlyClient = 0b10
    FullServerResponse = 0b1001
    AudioOnlyServer = 0b1011
    FrontEndResultServer = 0b1100
    Error = 0b1111

class MsgTypeFlagBits(IntEnum):
    NoSeq = 0
    PositiveSeq = 0b1
    LastNoSeq = 0b10
    NegativeSeq = 0b11
    WithEvent = 0b100

class VersionBits(IntEnum):
    Version1 = 1

class HeaderSizeBits(IntEnum):
    HeaderSize4 = 1

class SerializationBits(IntEnum):
    Raw = 0
    JSON = 0b1

class CompressionBits(IntEnum):
    None_ = 0

class EventType(IntEnum):
    None_ = 0
    StartConnection = 1
    FinishConnection = 2
    ConnectionStarted = 50
    ConnectionFailed = 51
    ConnectionFinished = 52
    StartSession = 100
    CancelSession = 101
    FinishSession = 102
    SessionStarted = 150
    SessionCanceled = 151
    SessionFinished = 152
    SessionFailed = 153
    UsageResponse = 154
    TaskRequest = 200
    TTSSentenceStart = 350
    TTSSentenceEnd = 351
    TTSResponse = 352

class SystemState(Enum):
    IDLE = 0
    BROADCASTING = 1
    INTERRUPTING = 2

# ========== 全局调度中心（线程安全） ==========
system_state = SystemState.IDLE

# ========== 二进制协议编解码（官方标准） ==========
@dataclass
class Message:
    version: VersionBits = VersionBits.Version1
    header_size: HeaderSizeBits = HeaderSizeBits.HeaderSize4
    type: MsgType = MsgType.Invalid
    flag: MsgTypeFlagBits = MsgTypeFlagBits.NoSeq
    serialization: SerializationBits = SerializationBits.JSON
    compression: CompressionBits = CompressionBits.None_

    event: EventType = EventType.None_
    session_id: str = ""
    connect_id: str = ""
    sequence: int = 0
    error_code: int = 0

    payload: bytes = b""

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        if len(data) < 3:
            raise ValueError(f"数据过短: {len(data)} 字节")
        type_and_flag = data[1]
        msg_type = MsgType(type_and_flag >> 4)
        flag = MsgTypeFlagBits(type_and_flag & 0b00001111)
        msg = cls(type=msg_type, flag=flag)
        msg._unmarshal(data)
        return msg

    def marshal(self) -> bytes:
        buffer = io.BytesIO()
        header = [(self.version << 4) | self.header_size, (self.type << 4) | self.flag, (self.serialization << 4) | self.compression]
        header_size = 4 * self.header_size
        if padding := header_size - len(header):
            header.extend([0] * padding)
        buffer.write(bytes(header))
        for writer in self._get_writers():
            writer(buffer)
        return buffer.getvalue()

    def _unmarshal(self, data: bytes) -> None:
        buffer = io.BytesIO(data)
        version_and_header_size = buffer.read(1)[0]
        self.version = VersionBits(version_and_header_size >> 4)
        self.header_size = HeaderSizeBits(version_and_header_size & 0b00001111)
        buffer.read(1)
        serialization_compression = buffer.read(1)[0]
        self.serialization = SerializationBits(serialization_compression >> 4)
        self.compression = CompressionBits(serialization_compression & 0b00001111)
        header_size = 4 * self.header_size
        read_size = 3
        if padding_size := header_size - read_size:
            buffer.read(padding_size)
        for reader in self._get_readers():
            reader(buffer)

    def _get_writers(self) -> list:
        writers = []
        if self.flag == MsgTypeFlagBits.WithEvent:
            writers.extend([self._write_event, self._write_session_id])
        if self.type in [MsgType.FullClientRequest, MsgType.FullServerResponse, MsgType.FrontEndResultServer, MsgType.AudioOnlyClient, MsgType.AudioOnlyServer]:
            if self.flag in [MsgTypeFlagBits.PositiveSeq, MsgTypeFlagBits.NegativeSeq]:
                writers.append(self._write_sequence)
        elif self.type == MsgType.Error:
            writers.append(self._write_error_code)
        writers.append(self._write_payload)
        return writers

    def _get_readers(self) -> list:
        readers = []
        if self.type in [MsgType.FullClientRequest, MsgType.FullServerResponse, MsgType.FrontEndResultServer, MsgType.AudioOnlyClient, MsgType.AudioOnlyServer]:
            if self.flag in [MsgTypeFlagBits.PositiveSeq, MsgTypeFlagBits.NegativeSeq]:
                readers.append(self._read_sequence)
        elif self.type == MsgType.Error:
            readers.append(self._read_error_code)
        if self.flag == MsgTypeFlagBits.WithEvent:
            readers.extend([self._read_event, self._read_session_id, self._read_connect_id])
        readers.append(self._read_payload)
        return readers

    def _write_event(self, buffer: io.BytesIO) -> None:
        buffer.write(struct.pack(">i", self.event))
    def _write_session_id(self, buffer: io.BytesIO) -> None:
        if self.event in [EventType.StartConnection, EventType.FinishConnection, EventType.ConnectionStarted, EventType.ConnectionFailed]:
            return
        sid_bytes = self.session_id.encode("utf-8")
        buffer.write(struct.pack(">I", len(sid_bytes)))
        if sid_bytes:
            buffer.write(sid_bytes)
    def _write_sequence(self, buffer: io.BytesIO) -> None:
        buffer.write(struct.pack(">i", self.sequence))
    def _write_error_code(self, buffer: io.BytesIO) -> None:
        buffer.write(struct.pack(">I", self.error_code))
    def _write_payload(self, buffer: io.BytesIO) -> None:
        buffer.write(struct.pack(">I", len(self.payload)))
        buffer.write(self.payload)

    def _read_event(self, buffer: io.BytesIO) -> None:
        eb = buffer.read(4)
        if eb: self.event = EventType(struct.unpack(">i", eb)[0])
    def _read_session_id(self, buffer: io.BytesIO) -> None:
        if self.event in [EventType.StartConnection, EventType.FinishConnection, EventType.ConnectionStarted, EventType.ConnectionFailed, EventType.ConnectionFinished]:
            return
        sb = buffer.read(4)
        if sb:
            sz = struct.unpack(">I", sb)[0]
            if sz>0: self.session_id = buffer.read(sz).decode("utf-8")
    def _read_connect_id(self, buffer: io.BytesIO) -> None:
        if self.event in [EventType.ConnectionStarted, EventType.ConnectionFailed, EventType.ConnectionFinished]:
            sb = buffer.read(4)
            if sb:
                sz = struct.unpack(">I", sb)[0]
                if sz>0: self.connect_id = buffer.read(sz).decode("utf-8")
    def _read_sequence(self, buffer: io.BytesIO) -> None:
        sb = buffer.read(4)
        if sb: self.sequence = struct.unpack(">i", sb)[0]
    def _read_error_code(self, buffer: io.BytesIO) -> None:
        sb = buffer.read(4)
        if sb: self.error_code = struct.unpack(">I", sb)[0]
    def _read_payload(self, buffer: io.BytesIO) -> None:
        sb = buffer.read(4)
        if sb:
            sz = struct.unpack(">I", sb)[0]
            if sz>0: self.payload = buffer.read(sz)

# ========== 协议辅助函数 ==========
async def _receive_message(ws) -> Message:
    data = await ws.recv()
    if isinstance(data, str):
        raise ValueError(f"收到非二进制消息:{data}")
    return Message.from_bytes(data)

async def _wait_for_event(ws, msg_type: MsgType, event_type: EventType) -> Message:
    while True:
        msg = await _receive_message(ws)
        if msg.type == msg_type and msg.event == event_type:
            return msg
        raise RuntimeError(f"异常消息:{msg.type}:{msg.event}")

def _build_start_connection() -> Message:
    msg = Message(type=MsgType.FullClientRequest, flag=MsgTypeFlagBits.WithEvent)
    msg.event = EventType.StartConnection
    msg.payload = b"{}"
    return msg

def _build_finish_connection() -> Message:
    msg = Message(type=MsgType.FullClientRequest, flag=MsgTypeFlagBits.WithEvent)
    msg.event = EventType.FinishConnection
    msg.payload = b"{}"
    return msg

def _build_start_session(session_id: str, speaker: str, fmt: str, sr: int) -> Message:
    payload = {
        "user": {"uid": str(uuid.uuid4())},
        "namespace": "BidirectionalTTS",
        "event": EventType.StartSession,
        "req_params": {
            "speaker": speaker,
            "audio_params": {"format": fmt, "sample_rate": sr, "enable_timestamp": True},
            "additions": '{"disable_markdown_filter": false}',
        }
    }
    import json
    msg = Message(type=MsgType.FullClientRequest, flag=MsgTypeFlagBits.WithEvent)
    msg.event = EventType.StartSession
    msg.session_id = session_id
    msg.payload = json.dumps(payload).encode("utf-8")
    return msg

def _build_task_request(session_id: str, text: str, speaker: str, fmt: str, sr: int) -> Message:
    payload = {
        "user": {"uid": str(uuid.uuid4())},
        "namespace": "BidirectionalTTS",
        "event": EventType.TaskRequest,
        "req_params": {"text": text, "speaker": speaker, "audio_params": {"format": fmt, "sample_rate": sr}}
    }
    import json
    msg = Message(type=MsgType.FullClientRequest, flag=MsgTypeFlagBits.WithEvent)
    msg.event = EventType.TaskRequest
    msg.session_id = session_id
    msg.payload = json.dumps(payload).encode("utf-8")
    return msg

def _build_finish_session(session_id: str) -> Message:
    msg = Message(type=MsgType.FullClientRequest, flag=MsgTypeFlagBits.WithEvent)
    msg.event = EventType.FinishSession
    msg.session_id = session_id
    msg.payload = b"{}"
    return msg

# ========== TTS引擎（兼容websockets11.0~16.0） ==========
class TTSEngine:
    TTS_URL = "wss://openspeech.bytedance.com/api/v3/tts/bidirection"

    def __init__(self):
        self.ws = None

    def _build_ws_headers(self):
        return {
            "X-Api-Key": config.tts_api_key,
            "X-Api-Resource-Id": config.tts_resource_id,
            "X-Api-Connect-Id": str(uuid.uuid4()),
        }

    async def connect(self):
        if self.ws and self.ws.close_code is None:
            return
        self.ws = await websockets.connect(
            self.TTS_URL, 
            additional_headers=self._build_ws_headers(), 
            max_size=10*1024*1024
        )
        await self.ws.send(_build_start_connection().marshal())
        await _wait_for_event(self.ws, MsgType.FullServerResponse, EventType.ConnectionStarted)
        logger.info("✅ TTS 全局连接建立成功")

    async def disconnect(self):
        if self.ws and self.ws.close_code is None:
            try:
                await asyncio.shield(self.ws.send(_build_finish_connection().marshal()))
            except Exception:
                pass
            try:
                await self.ws.close()
            except Exception:
                pass
        logger.info("✅ TTS 连接已安全关闭")

    async def synthesize_single_sentence(self, text: str) -> AsyncIterator[bytes]:
        """流式合成单句文本"""
        if not self.ws or self.ws.close_code is not None:
            await self.connect()

        session_id = str(uuid.uuid4())
        await self.ws.send(_build_start_session(
            session_id, 
            config.tts_speaker_id, 
            config.tts_format, 
            config.tts_sample_rate
        ).marshal())
        await _wait_for_event(self.ws, MsgType.FullServerResponse, EventType.SessionStarted)

        async def send_text():
            for c in text:
                await self.ws.send(_build_task_request(
                    session_id, c, 
                    config.tts_speaker_id, 
                    config.tts_format, 
                    config.tts_sample_rate
                ).marshal())
                await asyncio.sleep(0.0045)
            await self.ws.send(_build_finish_session(session_id).marshal())

        send_task = asyncio.create_task(send_text())
        try:
            while True:
                msg = await _receive_message(self.ws)
                if msg.type == MsgType.FullServerResponse:
                    if msg.event == EventType.SessionFinished:
                        break
                elif msg.type == MsgType.AudioOnlyServer and msg.payload:
                    yield msg.payload
        finally:
            await send_task

    async def pre_synthesize_single_sentence(self, text: str) -> List[bytes]:
        """预合成单句文本，返回所有音频块列表"""
        chunks = []
        async for chunk in self.synthesize_single_sentence(text):
            chunks.append(chunk)
        return chunks

# ========== PCM播放器【句尾完美修复版】 ==========
class RealTimePCMPlayer:
    # 20ms标准帧：24000采样率 × 0.02秒 = 480采样点 × 2字节/采样 = 960字节/帧
    FRAME_SAMPLES = 480
    FRAME_BYTES = FRAME_SAMPLES * 2
    BUFFER_CACHE = b""

    def __init__(self):
        self.p = pyaudio.PyAudio()
        # 硬件缓冲区与帧长对齐，彻底消除空窗杂音
        self.stream = self.p.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=config.tts_sample_rate,
            output=True,
            frames_per_buffer=self.FRAME_SAMPLES
        )

    def play_chunk(self, raw_pcm: bytes):
        try:
            self.BUFFER_CACHE += raw_pcm
            # 凑够完整帧再写入声卡，剩余字节留在缓存
            while len(self.BUFFER_CACHE) >= self.FRAME_BYTES:
                write_data = self.BUFFER_CACHE[:self.FRAME_BYTES]
                self.BUFFER_CACHE = self.BUFFER_CACHE[self.FRAME_BYTES:]
                self.stream.write(write_data)
        except Exception:
            pass

    def play_chunks(self, chunks: List[bytes]):
        """播放预合成好的音频块列表"""
        for chunk in chunks:
            self.play_chunk(chunk)
        # 每一段播放完立刻刷完所有缓存，确保尾音完整
        self.flush()

    def flush(self):
        """强制刷完所有缓存音频，不足一帧补静音"""
        try:
            if len(self.BUFFER_CACHE) > 0:
                pad_length = self.FRAME_BYTES - len(self.BUFFER_CACHE)
                self.stream.write(self.BUFFER_CACHE + bytes(pad_length))
                self.BUFFER_CACHE = b""
        except Exception:
            pass

    def close(self):
        try:
            # 退出前最后刷一次缓存
            self.flush()
            self.stream.stop_stream()
            self.stream.close()
            self.p.terminate()
            logger.info("✅ 播放器已关闭")
        except Exception:
            pass

# ========== 文本处理（完整句子分割） ==========
def load_and_split_text():
    fp = Path(config.broadcast_text_file)
    if not fp.exists():
        raise FileNotFoundError(f"未找到文件: {config.broadcast_text_file}")
    
    with open(fp, "r", encoding="utf-8") as f:
        raw_content = f.read().strip()
    
    split_pattern = r"([。！？；])"
    parts = re.split(split_pattern, raw_content)
    sentences, buffer = [], ""
    
    for part in parts:
        buffer += part
        if re.match(split_pattern, part):
            clean_sent = buffer.strip()
            if clean_sent:
                sentences.append(clean_sent)
            buffer = ""
    
    if buffer.strip():
        sentences.append(buffer.strip())
    
    logger.info(f"✅ 文本拆分完毕，共 {len(sentences)} 段完整话术")
    return sentences

# ========== 主广播流程【智能弹幕版】 ==========
async def main_broadcast():
    global system_state
    sentences = load_and_split_text()
    
    tts = TTSEngine()
    player = RealTimePCMPlayer()
    
    # 扫描缓存目录，用于随机插播
    interject_candidates = []
    cache_dir = Path(config.broadcast_cache_dir)
    if cache_dir.exists():
        interject_candidates = [
            f for f in cache_dir.glob("*.pcm")
            if f.stat().st_size > 8192
        ]
        if interject_candidates:
            logger.info(f"🎲 找到 {len(interject_candidates)} 段缓存可用于随机插播")
    
    try:
        await tts.connect()
        system_state = SystemState.BROADCASTING
        logger.info("🚀 开始实时语音播报")
        
        for idx, sent in enumerate(sentences, 1):
            # 生成缓存键并检查本地缓存
            cache_key = get_cache_key(sent)
            cached_audio = load_from_cache(cache_key)
            
            if cached_audio is not None:
                logger.info(f"🎙️ 使用缓存第{idx}段: {sent[:100]}...")
                player.play_chunk(cached_audio)
                player.flush()
            else:
                logger.info(f"🎙️ 合成第{idx}段: {sent[:100]}...")
                # 流式合成：来一块播一块，不等全合成完
                audio_chunks = []
                async for chunk in tts.synthesize_single_sentence(sent):
                    player.play_chunk(chunk)
                    audio_chunks.append(chunk)
                player.flush()
                # 保存缓存以便下次直播
                save_to_cache(cache_key, b"".join(audio_chunks))
            
            # 句间插播：弹幕回复 + 随机缓存
            while not interrupt_queue.empty():
                reply_text = await interrupt_queue.get()
                logger.info(f"🔊 插播弹幕: {reply_text[:100]}...")
                try:
                    async for chunk in tts.synthesize_single_sentence(reply_text):
                        player.play_chunk(chunk)
                    player.flush()
                except Exception as e:
                    logger.error(f"❌ 弹幕插播失败: {e}")
            
            # 随机插播（缓存 / LLM / 无——三种互斥）
            roll = random.random()
            if roll < 0.08 and interject_candidates:
                interject_file = random.choice(interject_candidates)
                try:
                    logger.info(f"🎲 随机插播cache: {interject_file.stem}")
                    with open(interject_file, "rb") as f:
                        player.play_chunk(f.read())
                    player.flush()
                except Exception as e:
                    logger.error(f"❌ 随机插播失败: {e}")
            elif roll < 0.8:
                try:
                    from tts_random_llm import generate_and_play_interject
                    await generate_and_play_interject(tts, player, sent)
                except ImportError:
                    pass
        
        logger.info("🎉 全文播报完成！")
    except KeyboardInterrupt:
        logger.info("🛑 手动退出程序")
    except Exception as err:
        logger.error(f"❌ 程序运行异常: {err}", exc_info=True)
    finally:
        await tts.disconnect()
        player.close()
        system_state = SystemState.IDLE

# ========== 弹幕插播接口 ==========
interrupt_queue = asyncio.Queue()          # 待播报的回复文本（外部喂入）
danmu_input_queue = asyncio.Queue()        # 原始弹幕输入（手动/WebSocket）

async def add_interrupt(text: str):
    """外部接口：向口播插入一条语音回复"""
    await interrupt_queue.put(text)
    logger.info(f"📥 插播入队: {text[:30]}...")

async def danmu_handler_loop():
    """弹幕处理循环：输入 → 频率/LLM → 入插播队列（延迟导入避免循环依赖）"""
    import tts_reply_barrage as barrage

    while True:
        content = await danmu_input_queue.get()
        if not barrage.should_reply(content):
            continue

        reply_text = await barrage.generate_reply(content)

        # 同步更新频率状态
        barrage.last_reply_time = time.time()
        barrage.reply_count_in_minute += 1
        barrage.replied_contents.add(content.strip().lower())

        await add_interrupt(reply_text)

async def manual_danmu_input():
    """手动输入模拟弹幕（调试用，取消注释即可启用）"""
    loop = asyncio.get_running_loop()
    while True:
        text = await loop.run_in_executor(None, input, "📱 弹幕 >> ")
        if text.strip().lower() in ("q", "quit", "exit"):
            break
        await danmu_input_queue.put(text.strip())
    logger.info("🛑 手动弹幕输入已退出")

# ========== 程序入口（口播 + 弹幕） ==========
async def run_broadcast():
    """启动口播 + 弹幕处理"""
    await asyncio.gather(
        main_broadcast(),
        danmu_handler_loop(),
        # manual_danmu_input(),        # 取消注释启用手动弹幕输入
    )

if __name__ == "__main__":
    try:
        asyncio.run(run_broadcast())
    except KeyboardInterrupt:
        pass