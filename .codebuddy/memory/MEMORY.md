# 项目记忆

## 项目概述
AI无人口播直播系统（纯音频），基于字节跳动TTS + Python asyncio 实现。

## 重要决策 (2026-06-06)
- **口播系统** (tts_broadcast.py): 方案一流式合成，来一块播一块，不攒句
- **弹幕回复** (tts_barrage.py): 纯LLM方案，去掉所有关键词匹配规则
  - LLM: 豆包 `doubao-seed-2-0-lite-260428`，stream=true, thinking=disabled
  - LLM 流式 token 逐字喂 TTS，零积累零等待
  - 商品知识来自 `answer.txt`，拼入 System Prompt
- 弹幕回复完全独立于口播系统，不修改 tts_broadcast.py

## 工作原则
- 先提案，确认后再动手
- 只改指定范围，不擅自主张
