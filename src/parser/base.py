"""
解析引擎框架。

每个字段有一个 FieldExtractor 接口，可以注册多个实现（规则、LLM 等），
通过 config 选择使用哪个实现，方便 A/B 测试。
"""
from abc import ABC, abstractmethod


class FieldExtractor(ABC):
    """字段提取器基类。"""

    name: str = "base"

    @abstractmethod
    def extract(self, text: str, html: str, context: dict) -> object:
        """
        从文本/HTML中提取信息。

        Args:
            text: 去除标签后的纯文本
            html: 原始 HTML
            context: 上下文信息 (url, title, search_query 等)

        Returns:
            提取到的值，类型取决于具体字段
        """
        ...


class LLMFieldExtractor(FieldExtractor):
    """大模型提取器接口（预留）。子类实现具体的 LLM 调用。"""

    name = "llm"

    def extract(self, text: str, html: str, context: dict) -> object:
        return None


PARSER_VERSION = "rule_v1"
