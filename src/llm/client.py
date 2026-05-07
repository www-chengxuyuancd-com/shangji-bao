"""大模型调用客户端 - 支持 DeepSeek / OpenAI 兼容 API"""

import json
import logging

from openai import OpenAI

from src.db.prisma_client import get_prisma

logger = logging.getLogger(__name__)

DEFAULT_LABELING_PROMPT = """你是一个招投标信息分类助手。请判断下面的公告是否值得用户跟进。

## 用户的业务关键词
{business_keywords}

只要标题或正文涉及上述任何一个业务领域，即视为业务相关。

## 判定为"相关 (label=1)"的条件（同时满足）
1. 内容明确属于上述业务关键词覆盖的领域之一（建设/采购/施工对象与该领域有直接关系，不能只是顺带提一句）。
2. 公告类型属于"招标 / 采购阶段"，例如：招标公告、招标计划、采购公告、竞争性磋商、竞争性谈判、询价、比选、邀请招标、预招标、单一来源公示、采购意向、公开招标、磋商公告、询价公告 等。

## 判定为"不相关 (label=0)"的常见情况
- 标题/正文与所有业务关键词都无直接关系（例如绿化、物业管理、清洁服务、单纯土建、与业务无关的政府文件等）。
- 公告类型属于"招标后置阶段"：中标公告、成交公告、结果公示、变更公告、废标公告、合同公示、签约履行、验收公告、开标记录 等（用户只关心"还能投标的"，已成交/已结束的不算）。
- 招聘信息、新闻报道、政策法规、培训通知、领导讲话等非采购信息。
- 内容过于笼统、无法判断具体业务范围（宁缺毋滥，标 0）。

## 输入信息
- 标题：{title}
- 正文摘要：{content}

## 输出要求
严格只输出一行 JSON，不要任何其他文字、不要 Markdown 代码块：
{{"label": 1 或 0, "reason": "一句话理由（≤30字），需指出关键证据"}}"""


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


def _get_business_keywords() -> list[str]:
    """读取所有启用的业务关键词。失败时返回空列表（让上层用兜底文案）。"""
    try:
        prisma = get_prisma()
        kws = prisma.searchkeyword.find_many(where={"enabled": True})
        return [k.keyword for k in kws if k.keyword]
    except Exception as e:
        logger.warning("Read business keywords failed: %s", e)
        return []


def _safe_format(template: str, **kwargs) -> str:
    """
    安全 format：模板里出现的占位符如果在 kwargs 里没给值，就保持原样输出，
    不会因为旧/新 prompt 不匹配而抛 KeyError。
    """
    class _SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"
    import string
    return string.Formatter().vformat(template, (), _SafeDict(**kwargs))


def predict_label(
    title: str,
    content: str,
    search_query: str | None = None,
    business_keywords: list[str] | None = None,
) -> dict | None:
    """
    调用大模型预测单条记录的标签。
    返回 {"label": 0|1, "reason": "..."} 或 None（配置缺失/调用失败时）

    参数：
      title             - 标题
      content           - 正文摘要
      search_query      - 触发本条记录的搜索词（可空。比如网站直接抓的原文、force_url 等）
      business_keywords - 指定本次比对的业务关键词列表。不传则从数据库读 enabled 的全部
                          SearchKeyword 作为业务领域。
    """
    cfg = _get_llm_config()
    if not cfg:
        return None

    prompt_template = _get_labeling_prompt()

    # 业务关键词：调用方没传 → 用数据库 enabled 全部
    if business_keywords is None:
        business_keywords = _get_business_keywords()
    business_kw_text = "、".join(business_keywords) if business_keywords else "（未配置业务关键词，仅判断是否为招标/采购阶段公告）"

    # search_query：如果是 force_url / 空 / 网站直抓这种"伪查询"，标记为 N/A
    sq = (search_query or "").strip()
    if not sq or sq.lower() in ("force_url", "manual", "website", "n/a", "none", "未知"):
        sq = "N/A"

    content_snippet = (content or "")[:1500]
    user_message = _safe_format(
        prompt_template,
        business_keywords=business_kw_text,
        search_query=sq,
        title=title or "无标题",
        content=content_snippet or "无内容",
    )

    try:
        client = OpenAI(api_key=cfg["api_key"], base_url=cfg["base_url"], timeout=60)
        kwargs = {
            "model": cfg["model"],
            "messages": [
                {"role": "system", "content": "你是一个专业的招投标信息分类助手，只输出 JSON。"},
                {"role": "user", "content": user_message},
            ],
            "temperature": cfg["temperature"],
            "max_tokens": cfg["max_tokens"],
            "response_format": {"type": "json_object"},
        }
        resp = client.chat.completions.create(**kwargs)
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


def batch_predict_labels(
    items: list[dict],
    business_keywords: list[str] | None = None,
) -> list[dict | None]:
    """
    批量预测标签。items 格式: [{"id": ..., "title": ..., "content": ..., "searchQuery": ...}, ...]
    返回与 items 等长的列表，每项为 {"label": 0|1, "reason": "..."} 或 None。

    business_keywords 不传则只在批次开始时读一次数据库（比 N 次 predict_label 各自读 PG 高效得多）。
    """
    if business_keywords is None:
        business_keywords = _get_business_keywords()
    results = []
    for item in items:
        r = predict_label(
            title=item.get("title", ""),
            content=item.get("content", ""),
            search_query=item.get("searchQuery", ""),
            business_keywords=business_keywords,
        )
        results.append(r)
    return results
