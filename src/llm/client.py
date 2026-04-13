"""大模型调用客户端 - 支持 DeepSeek / OpenAI 兼容 API"""

import json
import logging

from openai import OpenAI

from src.db.prisma_client import get_prisma

logger = logging.getLogger(__name__)

DEFAULT_LABELING_PROMPT = """你是一个招投标信息分类助手。请判断以下信息是否与用户的业务相关。

## 判断规则
1. 搜索词代表用户关注的业务领域。第一个关键词（如"弱电"）是核心业务词，标题或内容必须与该业务领域直接相关。
2. 后续关键词（如"招标"、"竞争性磋商"、"竞争性谈判"、"询价"、"比选"等）表示公告类型，内容应属于这些采购/招标类公告。
3. 同时满足以上两点才算"相关"。
4. 以下情况标为"不相关"：
   - 标题/内容与核心业务词无关（如搜索"弱电"但内容是纯土建、绿化、物业等）
   - 属于中标公告、结果公告、变更公告、废标公告等非招标阶段公告
   - 内容为招聘信息、新闻报道、政策文件等非招投标信息
   - 内容过于笼统无法判断具体业务领域

## 输入信息
- 搜索词: {search_query}
- 标题: {title}
- 内容摘要: {content}

## 输出要求
请严格按以下 JSON 格式输出，不要输出其他内容：
{{"label": 1 或 0, "reason": "简短理由"}}

其中 label=1 表示相关，label=0 表示不相关。"""


def _get_llm_config():
    """从数据库获取已启用的 LLM 配置"""
    prisma = get_prisma()
    cfg = prisma.llmconfig.find_first(where={"enabled": True})
    if not cfg:
        return None
    return {
        "api_key": cfg.apiKey,
        "base_url": cfg.baseUrl,
        "model": cfg.model,
        "temperature": cfg.temperature,
        "max_tokens": cfg.maxTokens,
    }


def _get_labeling_prompt():
    """获取默认标注提示词，若数据库无配置则用内置默认"""
    prisma = get_prisma()
    p = prisma.labelingprompt.find_first(where={"isDefault": True})
    if p:
        return p.prompt
    return DEFAULT_LABELING_PROMPT


def predict_label(title: str, content: str, search_query: str) -> dict | None:
    """
    调用大模型预测单条记录的标签。
    返回 {"label": 0|1, "reason": "..."} 或 None（配置缺失/调用失败时）
    """
    cfg = _get_llm_config()
    if not cfg:
        return None

    prompt_template = _get_labeling_prompt()

    content_snippet = (content or "")[:800]
    user_message = prompt_template.format(
        search_query=search_query or "未知",
        title=title or "无标题",
        content=content_snippet or "无内容",
    )

    try:
        client = OpenAI(api_key=cfg["api_key"], base_url=cfg["base_url"], timeout=90)
        resp = client.chat.completions.create(
            model=cfg["model"],
            messages=[
                {"role": "system", "content": "你是一个专业的招投标信息分类助手，只输出 JSON。"},
                {"role": "user", "content": user_message},
            ],
            temperature=cfg["temperature"],
            max_tokens=cfg["max_tokens"],
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(text)
        label = int(result.get("label", -1))
        if label not in (0, 1):
            return None
        return {"label": label, "reason": result.get("reason", "")}
    except Exception as e:
        logger.warning("LLM predict_label failed: %s", e)
        return None


def batch_predict_labels(items: list[dict]) -> list[dict | None]:
    """
    批量预测标签。items 格式: [{"id": ..., "title": ..., "content": ..., "searchQuery": ...}, ...]
    返回与 items 等长的列表，每项为 {"label": 0|1, "reason": "..."} 或 None。
    """
    results = []
    for item in items:
        r = predict_label(
            title=item.get("title", ""),
            content=item.get("content", ""),
            search_query=item.get("searchQuery", ""),
        )
        results.append(r)
    return results
