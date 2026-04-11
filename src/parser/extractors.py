"""
规则提取器实现。

对各字段使用正则 + 启发式规则提取。
"""
import re
from datetime import datetime

from src.parser.base import FieldExtractor

# ====================== 工具函数 ======================

_REMOVE_TAGS_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def html_to_text(html: str) -> str:
    """粗略去除 HTML 标签，返回纯文本。"""
    text = _REMOVE_TAGS_RE.sub(" ", html)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


# ====================== 日期提取 ======================

_DATE_PATTERNS = [
    # 2024年03月15日
    re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"),
    # 2024-03-15 or 2024/03/15
    re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"),
]

_PUBLISH_HINTS = [
    "发布时间", "发布日期", "公告日期", "公告时间", "发文日期", "发布于",
    "公示日期", "公示时间", "信息时间", "发布时间：", "日期",
]


class PublishDateExtractor(FieldExtractor):
    name = "rule_publish_date"

    def extract(self, text: str, html: str, context: dict) -> datetime | None:
        for hint in _PUBLISH_HINTS:
            idx = text.find(hint)
            if idx == -1:
                continue
            snippet = text[idx:idx + 80]
            for pat in _DATE_PATTERNS:
                m = pat.search(snippet)
                if m:
                    try:
                        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                    except ValueError:
                        continue

        for pat in _DATE_PATTERNS:
            m = pat.search(text[:2000])
            if m:
                try:
                    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                except ValueError:
                    continue
        return None


# ====================== 招标方提取 ======================

_BIDDER_PATTERNS = [
    re.compile(r"(?:招标人|采购人|采购单位|招标单位|项目业主|建设单位|发包人|甲方)\s*[:：]\s*(.{2,60}?)(?:[,，。\n\r<]|$)"),
]


class BidderExtractor(FieldExtractor):
    name = "rule_bidder"

    def extract(self, text: str, html: str, context: dict) -> str | None:
        for pat in _BIDDER_PATTERNS:
            m = pat.search(text)
            if m:
                val = m.group(1).strip()
                val = re.sub(r"\s+", "", val)
                if 2 <= len(val) <= 50:
                    return val
        return None


# ====================== 地点提取 ======================

_LOCATION_PATTERNS = [
    re.compile(r"(?:项目地点|项目地址|工程地点|实施地点|交货地点|服务地点|地\s*点)\s*[:：]\s*(.{2,80}?)(?:[,，。\n\r<;；]|$)"),
]

_LOCATION_SUFFIX_RE = re.compile(r"([\u4e00-\u9fff]{2,}(?:省|自治区|市|区|县|镇|乡|村|街道))")


class LocationExtractor(FieldExtractor):
    name = "rule_location"

    def extract(self, text: str, html: str, context: dict) -> str | None:
        for pat in _LOCATION_PATTERNS:
            m = pat.search(text)
            if m:
                val = m.group(1).strip()
                val = re.sub(r"\s+", "", val)
                if len(val) <= 80:
                    return val

        matches = _LOCATION_SUFFIX_RE.findall(text[:3000])
        if matches:
            seen = []
            for loc in matches:
                if loc not in seen and len(loc) >= 3:
                    seen.append(loc)
                if len(seen) >= 3:
                    break
            if seen:
                return "、".join(seen)
        return None


# ====================== 投标时间提取 ======================

_BID_START_HINTS = ["投标开始", "报名开始", "报名时间", "获取招标文件时间"]
_BID_END_HINTS = ["投标截止", "投标结束", "截止时间", "递交截止", "提交投标文件截止", "开标时间"]

_DATETIME_PATTERNS = [
    re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*(\d{1,2})\s*[:时]\s*(\d{1,2})\s*分?"),
    re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s+(\d{1,2}):(\d{1,2})"),
    re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"),
    re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"),
]


def _extract_datetime_near(text: str, hints: list[str]) -> datetime | None:
    for hint in hints:
        idx = text.find(hint)
        if idx == -1:
            continue
        snippet = text[idx:idx + 120]
        for pat in _DATETIME_PATTERNS:
            m = pat.search(snippet)
            if m:
                groups = m.groups()
                try:
                    if len(groups) >= 5:
                        return datetime(int(groups[0]), int(groups[1]), int(groups[2]),
                                        int(groups[3]), int(groups[4]))
                    else:
                        return datetime(int(groups[0]), int(groups[1]), int(groups[2]))
                except ValueError:
                    continue
    return None


class BidStartTimeExtractor(FieldExtractor):
    name = "rule_bid_start"

    def extract(self, text: str, html: str, context: dict) -> datetime | None:
        return _extract_datetime_near(text, _BID_START_HINTS)


class BidEndTimeExtractor(FieldExtractor):
    name = "rule_bid_end"

    def extract(self, text: str, html: str, context: dict) -> datetime | None:
        return _extract_datetime_near(text, _BID_END_HINTS)


# ====================== 金额提取 ======================

_AMOUNT_PATTERNS = [
    re.compile(r"(?:预算金额|采购预算|项目预算|预算|控制价|标段金额|总金额|合同金额|中标金额|中标价|成交金额|成交价)\s*[:：]?\s*([\d,，.]+)\s*(万元|亿元|元)"),
    re.compile(r"([\d,，.]+)\s*(万元|亿元)"),
]


class AmountExtractor(FieldExtractor):
    name = "rule_amount"

    def extract(self, text: str, html: str, context: dict) -> dict | None:
        for pat in _AMOUNT_PATTERNS:
            m = pat.search(text)
            if m:
                raw_num = m.group(1).replace(",", "").replace("，", "")
                unit = m.group(2)
                try:
                    value = float(raw_num)
                    if unit == "亿元":
                        value *= 100_000_000
                    elif unit == "万元":
                        value *= 10_000
                    return {"display": f"{raw_num}{unit}", "value": value}
                except ValueError:
                    continue
        return None


# ====================== 联系方式提取 ======================

_PHONE_RE = re.compile(r"(?:联系电话|电\s*话|手\s*机|联系方式|联系人电话|咨询电话)\s*[:：]?\s*([\d\s\-()（）+]{7,20})")
_MOBILE_RE = re.compile(r"1[3-9]\d{9}")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_CONTACT_PERSON_RE = re.compile(r"(?:联\s*系\s*人|项目联系人|采购联系人)\s*[:：]\s*([\u4e00-\u9fff]{2,4})")


class ContactExtractor(FieldExtractor):
    name = "rule_contact"

    def extract(self, text: str, html: str, context: dict) -> str | None:
        parts = []
        pm = _CONTACT_PERSON_RE.search(text)
        if pm:
            parts.append(f"联系人: {pm.group(1)}")

        ph = _PHONE_RE.search(text)
        if ph:
            parts.append(f"电话: {ph.group(1).strip()}")
        else:
            mob = _MOBILE_RE.search(text)
            if mob:
                parts.append(f"手机: {mob.group(0)}")

        em = _EMAIL_RE.search(text)
        if em:
            parts.append(f"邮箱: {em.group(0)}")

        return "; ".join(parts) if parts else None


# ====================== 标题提取 ======================

class TitleExtractor(FieldExtractor):
    name = "rule_title"

    def extract(self, text: str, html: str, context: dict) -> str | None:
        title = context.get("title", "")
        if title and len(title.strip()) > 2:
            return title.strip()[:500]

        m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        if m:
            t = m.group(1).strip()
            if t:
                return t[:500]

        first_line = text[:200].strip().split("\n")[0].strip()
        return first_line[:500] if first_line else None


# ====================== 摘要提取 ======================

_JUNK_RE = re.compile(r"(?:首页|网站地图|版权所有|ICP备|Copyright|All Rights Reserved|关于我们|友情链接|设为首页|加入收藏)", re.IGNORECASE)


class SummaryExtractor(FieldExtractor):
    """提取前几段有意义的文字作为摘要。"""
    name = "rule_summary"

    def extract(self, text: str, html: str, context: dict) -> str | None:
        paragraphs = [p.strip() for p in text.split("\n") if len(p.strip()) > 15]
        meaningful = []
        for p in paragraphs:
            if _JUNK_RE.search(p):
                continue
            meaningful.append(p)
            if sum(len(s) for s in meaningful) > 300:
                break

        if meaningful:
            summary = " ".join(meaningful)
            return summary[:500]
        return text[:500] if len(text) > 10 else None


# ====================== 公告类型识别 ======================

NOTICE_TYPES = [
    ("中标", [
        re.compile(r"中标(公告|公示|结果|候选人|通知)", re.IGNORECASE),
        re.compile(r"(成交|中选)(公告|公示|结果)", re.IGNORECASE),
        re.compile(r"评标结果"),
        re.compile(r"定标(公告|结果|公示)"),
    ]),
    ("变更公告", [
        re.compile(r"(变更|更正|澄清|修改|延期|补充)(公告|通知|说明)", re.IGNORECASE),
        re.compile(r"(答疑|补遗)(公告|通知|文件)"),
    ]),
    ("废标公告", [
        re.compile(r"(废标|流标|终止|失败|作废)(公告|公示|通知)", re.IGNORECASE),
        re.compile(r"(采购|招标)(终止|中止|失败)"),
    ]),
    ("采购意向", [
        re.compile(r"采购意向", re.IGNORECASE),
        re.compile(r"(采购|需求)(计划|意向|预告)", re.IGNORECASE),
    ]),
    ("预招标", [
        re.compile(r"(预招标|资格预审|预审)(公告|通知|公示)", re.IGNORECASE),
        re.compile(r"(征集|意向征询|市场调研)(公告|通知)"),
    ]),
    ("合同", [
        re.compile(r"合同(公告|公示|签订|备案)", re.IGNORECASE),
        re.compile(r"(签约|订立合同)(公告|公示)"),
    ]),
    ("验收公告", [
        re.compile(r"(验收|履约)(公告|公示|结果|报告)", re.IGNORECASE),
        re.compile(r"(结算|决算)(公告|公示)"),
    ]),
    ("招标", [
        re.compile(r"招标(公告|通知|文件|邀请)", re.IGNORECASE),
        re.compile(r"(竞争性磋商|竞争性谈判|询价|比选|竞价|邀标)(公告|通知)", re.IGNORECASE),
        re.compile(r"(采购|招标|比价|议价)(公告|通知|项目)", re.IGNORECASE),
        re.compile(r"(公开招标|邀请招标|单一来源|竞价采购)"),
    ]),
]


class NoticeTypeExtractor(FieldExtractor):
    """通过标题和正文关键词匹配识别公告类型。"""
    name = "rule_notice_type"

    def extract(self, text: str, html: str, context: dict) -> str | None:
        title = context.get("title", "") or ""
        check_text = title + " " + text[:1500]

        for type_name, patterns in NOTICE_TYPES:
            for pat in patterns:
                if pat.search(check_text):
                    return type_name
        return None


# ====================== 关键词相关性 ======================

class RelevanceExtractor(FieldExtractor):
    """
    关键词匹配 + FastText 模型（如果可用）综合判断相关性。
    模型 confidence > 0.7 时以模型结果为主，否则降级到关键词匹配。
    """
    name = "rule_relevance"

    def extract(self, text: str, html: str, context: dict) -> dict:
        keywords = context.get("user_keywords", [])

        text_lower = text.lower()
        matched = []
        for kw in keywords:
            if kw.lower() in text_lower:
                matched.append(kw)
        kw_score = len(matched) / len(keywords) if keywords else 0
        kw_relevant = kw_score > 0

        ml_label = None
        ml_confidence = None
        try:
            from src.classifier.predictor import RelevancePredictor
            predictor = RelevancePredictor.get_instance()
            if predictor.available:
                pred = predictor.predict(text)
                if pred:
                    ml_label = pred["label"]
                    ml_confidence = pred["confidence"]
        except Exception:
            pass

        if ml_label is not None and ml_confidence is not None and ml_confidence > 0.7:
            is_relevant = ml_label == "relevant"
            final_score = ml_confidence if is_relevant else 1 - ml_confidence
        elif keywords:
            is_relevant = kw_relevant
            final_score = kw_score
        else:
            is_relevant = None
            final_score = None

        return {
            "is_relevant": is_relevant,
            "score": round(final_score, 2) if final_score is not None else None,
            "matched": ",".join(matched),
            "ml_label": ml_label,
            "ml_confidence": ml_confidence,
        }
