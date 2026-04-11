"""从 HTML 中提取同域名的链接。"""
import re
from urllib.parse import urljoin, urlparse

_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)

SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".bmp", ".webp",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".tar", ".gz", ".7z",
    ".mp3", ".mp4", ".avi", ".wmv", ".flv",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
}


def extract_same_domain_links(html: str, base_url: str, base_domain: str, max_links: int = 200) -> list[str]:
    """
    提取 HTML 中与 base_domain 同域名的链接。

    Args:
        html: HTML 源码
        base_url: 当前页面 URL（用于解析相对路径）
        base_domain: 基础域名（只提取同域名链接）
        max_links: 最多返回的链接数

    Returns:
        去重后的绝对 URL 列表
    """
    seen = set()
    result = []

    for match in _HREF_RE.finditer(html):
        href = match.group(1).strip()

        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        absolute = urljoin(base_url, href)

        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue

        if parsed.netloc != base_domain:
            continue

        ext = ""
        path = parsed.path.lower()
        dot_pos = path.rfind(".")
        if dot_pos > 0:
            ext = path[dot_pos:]
        if ext in SKIP_EXTENSIONS:
            continue

        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"

        if clean_url not in seen:
            seen.add(clean_url)
            result.append(clean_url)
            if len(result) >= max_links:
                break

    return result
