# ====================== TTS配置 ======================
tts_api_key = "3e369b38-a8eb-4f20-afe8-767cc1638b2a"
# 音色ID    zh_female_vv_uranus_bigtts
tts_speaker_id = "S_D7f5ktr22"
# seed-tts-2.0通用，S_开头音色填seed-icl-2.0         
tts_resource_id = "seed-icl-2.0"  
tts_format="pcm"
tts_sample_rate=24000

# ====================== 播报配置 ======================
broadcast_text_file="broadcast.txt"
broadcast_sentence_interval=0.05
broadcast_max_danmu_queue_size=4
broadcast_cache_dir="tts_broadcast_cache"

# ====================== 弹幕配置 ======================
danmu_max_replies_per_minute=6
danmu_min_reply_interval=4
danmu_max_same_content_reply=1
danmu_max_same_user_reply=2
danmu_enable_llm=False

# ====================== 豆包 LLM 配置 ======================
llm_api_key = "ark-88a8195a-2830-4600-93de-8132576a8696-5ed74"
llm_model = "doubao-seed-2-0-lite-260428"
llm_base_url = "https://ark.cn-beijing.volces.com/api/v3"

# ====================== 商品知识库 ======================
product_info_file = "answer.txt"

# ====================== 口播LLM生成 Prompt ======================
llm_content_system_prompt = (
    "你是卖女鞋的直播间主播，正在口播带货。"
    "根据对话历史和商品信息，自然地补一句，不要重复原话，控制在25字内~\n\n"
    "参考话术风格：\n"
    "- 姐妹们看这款爆款凉拖！\n"
    "- 专柜一百多，今天直播间只要39.9！\n"
    "- 源头工厂直发没有中间商~\n"
    "- 下雨天穿都稳得很，姐妹们放心穿~\n"
)