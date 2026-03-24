"""
验证码处理模块。

架构:
    CaptchaSolver (抽象基类)
    ├── ManualSolver        — 人工介入（开发调试用）
    ├── ThirdPartySolver    — 第三方打码平台通用接口
    └── (自定义 Solver)      — 后续根据需要扩展

支持的验证码类型:
    - image:    图片验证码
    - slider:   滑块验证码
    - click:    点选验证码
    - sms:      短信验证码

用法:
    solver = get_captcha_solver()
    result = solver.solve(captcha_type="image", image_data=b"...", meta={})
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

import requests

logger = logging.getLogger(__name__)


class CaptchaType(str, Enum):
    IMAGE = "image"
    SLIDER = "slider"
    CLICK = "click"
    SMS = "sms"


@dataclass
class CaptchaResult:
    success: bool
    answer: str | dict | None = None
    error: str | None = None
    cost_ms: int = 0


class CaptchaSolver(ABC):
    """验证码解决器基类。"""

    @abstractmethod
    def solve(
        self,
        captcha_type: str,
        image_data: bytes | None = None,
        image_url: str | None = None,
        page_url: str | None = None,
        meta: dict | None = None,
    ) -> CaptchaResult:
        """
        解决验证码。

        Args:
            captcha_type: 验证码类型 (image/slider/click/sms)
            image_data: 验证码图片的二进制数据
            image_url: 验证码图片的 URL（与 image_data 二选一）
            page_url: 验证码所在页面 URL
            meta: 额外的元数据（如 slider 的背景图、缺口坐标等）

        Returns:
            CaptchaResult
        """
        ...

    @abstractmethod
    def name(self) -> str:
        ...

    def supports(self, captcha_type: str) -> bool:
        return True


class ManualSolver(CaptchaSolver):
    """人工解码器，用于开发调试阶段。将验证码保存到本地文件并等待手动输入。"""

    def solve(self, captcha_type, image_data=None, image_url=None, page_url=None, meta=None):
        logger.warning("[ManualSolver] 需要人工处理验证码, page_url=%s, type=%s", page_url, captcha_type)

        if image_data:
            path = "/tmp/captcha_latest.png"
            with open(path, "wb") as f:
                f.write(image_data)
            logger.info("[ManualSolver] 验证码图片已保存到: %s", path)

        return CaptchaResult(success=False, error="需要人工处理")

    def name(self):
        return "manual"


class ThirdPartySolver(CaptchaSolver):
    """
    第三方打码平台通用接口。

    通过配置 api_url 和 api_key 对接不同的打码服务。
    目前支持的协议格式（可扩展）:
    - 通用 HTTP API: POST image_data + captcha_type, 返回 JSON {code, result}
    """

    def __init__(self, api_url: str, api_key: str, timeout: int = 30):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def solve(self, captcha_type, image_data=None, image_url=None, page_url=None, meta=None):
        import base64
        import time

        start = time.time()
        try:
            payload = {
                "api_key": self.api_key,
                "captcha_type": captcha_type,
                "page_url": page_url or "",
                "meta": meta or {},
            }

            files = None
            if image_data:
                payload["image_base64"] = base64.b64encode(image_data).decode()
            elif image_url:
                payload["image_url"] = image_url

            resp = requests.post(
                f"{self.api_url}/solve",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            cost = int((time.time() - start) * 1000)

            if data.get("success") or data.get("code") == 0:
                answer = data.get("result") or data.get("answer") or data.get("data")
                return CaptchaResult(success=True, answer=answer, cost_ms=cost)
            else:
                error = data.get("error") or data.get("message") or "Unknown error"
                return CaptchaResult(success=False, error=error, cost_ms=cost)

        except Exception as e:
            cost = int((time.time() - start) * 1000)
            logger.error("[ThirdPartySolver] Failed: %s", e)
            return CaptchaResult(success=False, error=str(e), cost_ms=cost)

    def name(self):
        return "third_party"


class CaptchaSolverChain:
    """
    验证码解决器链，依次尝试多个 Solver 直到成功。

    用法:
        chain = CaptchaSolverChain()
        chain.add(my_ocr_solver)           # 优先用本地 OCR
        chain.add(third_party_solver)       # 失败再用第三方
        result = chain.solve(...)
    """

    def __init__(self):
        self._solvers: list[CaptchaSolver] = []

    def add(self, solver: CaptchaSolver):
        self._solvers.append(solver)

    def solve(self, captcha_type, **kwargs) -> CaptchaResult:
        for solver in self._solvers:
            if not solver.supports(captcha_type):
                continue
            try:
                result = solver.solve(captcha_type=captcha_type, **kwargs)
                if result.success:
                    logger.info("[CaptchaChain] Solved by %s", solver.name())
                    return result
                logger.info("[CaptchaChain] %s failed: %s", solver.name(), result.error)
            except Exception as e:
                logger.warning("[CaptchaChain] %s error: %s", solver.name(), e)

        return CaptchaResult(success=False, error="All solvers failed")


_solver_chain: CaptchaSolverChain | None = None


def get_captcha_solver() -> CaptchaSolverChain:
    global _solver_chain
    if _solver_chain is None:
        _solver_chain = CaptchaSolverChain()
        _solver_chain.add(ManualSolver())
    return _solver_chain


def setup_third_party_solver(api_url: str, api_key: str, timeout: int = 30):
    """配置第三方打码服务并插入到 solver 链头部（优先使用）。"""
    chain = get_captcha_solver()
    solver = ThirdPartySolver(api_url=api_url, api_key=api_key, timeout=timeout)
    chain._solvers.insert(0, solver)
    logger.info("Third-party captcha solver configured: %s", api_url)
