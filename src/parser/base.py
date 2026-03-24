"""
解析器占位模块。

后续在此模块中实现对采集到的原始 HTML 的解析逻辑，例如：
- 提取招标金额
- 提取联系方式
- 提取截止日期
- 提取招标单位
- 提取项目详情
等结构化信息。
"""
from abc import ABC, abstractmethod


class BaseParser(ABC):
    """解析器基类，所有具体解析器需要继承此类。"""

    @abstractmethod
    def parse(self, html: str, url: str) -> dict:
        """
        解析 HTML 内容，返回结构化数据字典。

        Args:
            html: 原始 HTML 字符串
            url: 页面 URL

        Returns:
            解析后的结构化数据
        """
        ...

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        """判断此解析器是否能处理该 URL。"""
        ...


class DefaultParser(BaseParser):
    """默认解析器（占位），后续替换为实际解析逻辑。"""

    def parse(self, html: str, url: str) -> dict:
        return {
            "raw_length": len(html),
            "url": url,
            "parsed": False,
            "message": "尚未实现具体解析逻辑",
        }

    def can_handle(self, url: str) -> bool:
        return True


class ParserRegistry:
    """解析器注册表，根据 URL 自动选择合适的解析器。"""

    def __init__(self):
        self._parsers: list[BaseParser] = []
        self._default = DefaultParser()

    def register(self, parser: BaseParser):
        self._parsers.append(parser)

    def get_parser(self, url: str) -> BaseParser:
        for parser in self._parsers:
            if parser.can_handle(url):
                return parser
        return self._default

    def parse(self, html: str, url: str) -> dict:
        parser = self.get_parser(url)
        return parser.parse(html, url)


registry = ParserRegistry()
