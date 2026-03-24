"""
网站登录管理模块。

为需要登录才能访问的网站提供统一的认证管理:
- Cookie/Session 持久化（文件存储，避免频繁登录）
- 多账号轮换
- 登录状态检测与自动续期

用法:
    manager = get_login_manager()
    manager.register("example.com", ExampleLoginHandler(username="...", password="..."))

    cookies = manager.get_cookies("example.com")  # 自动登录并返回 cookies
"""
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from http.cookiejar import MozillaCookieJar
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

COOKIE_DIR = os.getenv("COOKIE_DIR", "/tmp/shangji_bao_cookies")


@dataclass
class LoginAccount:
    username: str
    password: str
    extra: dict = field(default_factory=dict)


@dataclass
class LoginSession:
    cookies: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)
    logged_in_at: float = 0
    expires_at: float = 0
    account: LoginAccount | None = None

    @property
    def is_expired(self) -> bool:
        if self.expires_at <= 0:
            return False
        return time.time() > self.expires_at

    @property
    def is_valid(self) -> bool:
        return bool(self.cookies) and not self.is_expired


class LoginHandler(ABC):
    """
    网站登录处理器基类。
    每个需要登录的网站应实现一个子类。
    """

    @abstractmethod
    def login(self, account: LoginAccount, session: requests.Session) -> LoginSession:
        """
        执行登录，返回包含 cookies 的 LoginSession。

        Args:
            account: 登录账号信息
            session: requests Session（可复用已有的 cookies）

        Returns:
            LoginSession
        """
        ...

    @abstractmethod
    def check_login(self, login_session: LoginSession, session: requests.Session) -> bool:
        """检查登录态是否仍然有效。"""
        ...

    @abstractmethod
    def site_domain(self) -> str:
        """返回此 handler 对应的域名。"""
        ...

    def session_ttl(self) -> int:
        """Session 有效期（秒），默认 2 小时。"""
        return 7200


class LoginManager:
    """登录管理器，集中管理多个网站的登录态。"""

    def __init__(self, cookie_dir: str = COOKIE_DIR):
        self._handlers: dict[str, LoginHandler] = {}
        self._accounts: dict[str, list[LoginAccount]] = {}
        self._sessions: dict[str, LoginSession] = {}
        self._cookie_dir = Path(cookie_dir)
        self._cookie_dir.mkdir(parents=True, exist_ok=True)

    def register(self, handler: LoginHandler, accounts: list[LoginAccount] | None = None):
        """注册一个网站的登录处理器和账号列表。"""
        domain = handler.site_domain()
        self._handlers[domain] = handler
        if accounts:
            self._accounts[domain] = accounts

    def add_account(self, domain: str, account: LoginAccount):
        self._accounts.setdefault(domain, []).append(account)

    def get_cookies(self, domain: str) -> dict:
        """获取指定域名的登录 cookies，如过期则自动重新登录。"""
        session_info = self._sessions.get(domain)

        if session_info and session_info.is_valid:
            return session_info.cookies

        cached = self._load_cookies(domain)
        if cached and cached.is_valid:
            handler = self._handlers.get(domain)
            if handler:
                req_session = requests.Session()
                req_session.cookies.update(cached.cookies)
                if handler.check_login(cached, req_session):
                    self._sessions[domain] = cached
                    return cached.cookies

        return self._do_login(domain)

    def get_headers(self, domain: str) -> dict:
        """获取指定域名登录后需要的自定义 headers（如 token）。"""
        session_info = self._sessions.get(domain)
        return session_info.headers if session_info else {}

    def _do_login(self, domain: str) -> dict:
        handler = self._handlers.get(domain)
        if not handler:
            logger.warning("No login handler for domain: %s", domain)
            return {}

        accounts = self._accounts.get(domain, [])
        if not accounts:
            logger.warning("No accounts for domain: %s", domain)
            return {}

        for account in accounts:
            try:
                req_session = requests.Session()
                login_session = handler.login(account, req_session)
                if login_session.is_valid:
                    login_session.account = account
                    if login_session.expires_at <= 0:
                        login_session.expires_at = time.time() + handler.session_ttl()
                    self._sessions[domain] = login_session
                    self._save_cookies(domain, login_session)
                    logger.info("Login success: %s @ %s", account.username, domain)
                    return login_session.cookies
                logger.warning("Login returned invalid session: %s @ %s", account.username, domain)
            except Exception as e:
                logger.error("Login failed: %s @ %s - %s", account.username, domain, e)

        logger.error("All accounts failed for domain: %s", domain)
        return {}

    def invalidate(self, domain: str):
        """标记某域名的登录态失效，下次 get_cookies 会重新登录。"""
        self._sessions.pop(domain, None)
        cookie_file = self._cookie_dir / f"{domain}.json"
        cookie_file.unlink(missing_ok=True)

    def _save_cookies(self, domain: str, session: LoginSession):
        cookie_file = self._cookie_dir / f"{domain}.json"
        data = {
            "cookies": session.cookies,
            "headers": session.headers,
            "logged_in_at": session.logged_in_at,
            "expires_at": session.expires_at,
            "username": session.account.username if session.account else "",
        }
        cookie_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    def _load_cookies(self, domain: str) -> LoginSession | None:
        cookie_file = self._cookie_dir / f"{domain}.json"
        if not cookie_file.exists():
            return None
        try:
            data = json.loads(cookie_file.read_text(encoding="utf-8"))
            return LoginSession(
                cookies=data.get("cookies", {}),
                headers=data.get("headers", {}),
                logged_in_at=data.get("logged_in_at", 0),
                expires_at=data.get("expires_at", 0),
            )
        except Exception as e:
            logger.warning("Failed to load cookies for %s: %s", domain, e)
            return None


_manager: LoginManager | None = None


def get_login_manager() -> LoginManager:
    global _manager
    if _manager is None:
        _manager = LoginManager()
    return _manager
