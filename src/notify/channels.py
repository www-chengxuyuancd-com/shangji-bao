"""
通知渠道实现。

QQ 渠道：兼容 OneBot v11 协议（go-cqhttp / Lagrange.OneBot / NapCat 等）。
短信渠道：支持互亿无线 / 阿里云短信，可配置多个收件人手机号。
"""
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import uuid
from base64 import b64encode
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


class BaseChannel:
    def send(self, config: dict, title: str, content: str) -> bool:
        raise NotImplementedError


class QQChannel(BaseChannel):
    """
    QQ 消息渠道（OneBot v11 HTTP API）。

    config:
    {
        "api_url": "http://127.0.0.1:3000",
        "message_type": "group",
        "target_id": "123456789",
        "access_token": ""
    }
    """

    def send(self, config: dict, title: str, content: str) -> bool:
        api_url = config.get("api_url", "").rstrip("/")
        msg_type = config.get("message_type", "group")
        target_id = config.get("target_id", "")
        token = config.get("access_token", "")

        if not api_url or not target_id:
            raise ValueError("QQ 渠道缺少 api_url 或 target_id")

        endpoint = f"{api_url}/send_msg"
        message = f"📢 {title}\n\n{content}"

        payload = {"message_type": msg_type, "message": message}
        if msg_type == "group":
            payload["group_id"] = int(target_id)
        else:
            payload["user_id"] = int(target_id)

        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        data = resp.json()
        if data.get("status") == "ok" or data.get("retcode") == 0:
            return True
        logger.warning("QQ send failed: %s", data)
        raise RuntimeError(f"QQ 发送失败: {data.get('msg', data.get('wording', str(data)))}")


class SMSChannel(BaseChannel):
    """
    短信渠道，支持多个收件人手机号，自动识别提供商。

    互亿无线 config:
    {
        "provider": "ihuyi",
        "account": "C12345678",
        "api_key": "xxxxxxxxxxxxxxxx",
        "phones": ["13800138000", "13900139000"],
        "sign_name": "商机宝"
    }

    阿里云短信 config:
    {
        "provider": "aliyun",
        "access_key_id": "LTAI...",
        "access_key_secret": "xxxxxxxx",
        "sign_name": "商机宝",
        "template_code": "SMS_123456",
        "phones": ["13800138000", "13900139000"]
    }

    通用 HTTP API config:
    {
        "provider": "generic",
        "api_url": "https://sms-api.example.com/send",
        "api_key": "your-key",
        "phones": ["13800138000"],
        "sign_name": "商机宝"
    }
    """

    def send(self, config: dict, title: str, content: str) -> bool:
        phones = config.get("phones", [])
        if isinstance(phones, str):
            phones = [p.strip() for p in phones.split(",") if p.strip()]
        if not phones:
            raise ValueError("短信渠道未配置手机号")

        provider = config.get("provider", "ihuyi")
        sign = config.get("sign_name", "商机宝")
        short_msg = f"【{sign}】{title}: {content[:160]}"

        errors = []
        success_count = 0
        for phone in phones:
            try:
                if provider == "ihuyi":
                    self._send_ihuyi(config, phone, short_msg)
                elif provider == "aliyun":
                    self._send_aliyun(config, phone, title, content[:160])
                else:
                    self._send_generic(config, phone, short_msg)
                success_count += 1
            except Exception as e:
                errors.append(f"{phone}: {e}")

        if success_count == 0:
            raise RuntimeError(f"所有手机号发送失败: {'; '.join(errors)}")
        if errors:
            logger.warning("部分手机号发送失败: %s", "; ".join(errors))
        return True

    def _send_ihuyi(self, config: dict, phone: str, message: str):
        """互亿无线 HTTP API: https://www.ihuyi.com/api/sms.html"""
        account = config.get("account", "")
        api_key = config.get("api_key", "")
        if not account or not api_key:
            raise ValueError("互亿无线缺少 account 或 api_key")

        resp = requests.post(
            "https://106.ihuyi.com/webservice/sms.php?method=Submit",
            data={
                "account": account,
                "password": api_key,
                "mobile": phone,
                "content": message,
                "format": "json",
            },
            timeout=10,
        )
        data = resp.json()
        code = data.get("code", -1)
        if int(code) == 2:
            return
        raise RuntimeError(f"互亿无线: {data.get('msg', data)}")

    def _send_aliyun(self, config: dict, phone: str, title: str, content: str):
        """阿里云短信 REST API（无需 SDK）。"""
        key_id = config.get("access_key_id", "")
        key_secret = config.get("access_key_secret", "")
        sign = config.get("sign_name", "")
        tpl = config.get("template_code", "")
        if not all([key_id, key_secret, sign, tpl]):
            raise ValueError("阿里云短信缺少必要配置")

        params = {
            "AccessKeyId": key_id,
            "Action": "SendSms",
            "Format": "JSON",
            "PhoneNumbers": phone,
            "SignName": sign,
            "SignatureMethod": "HMAC-SHA1",
            "SignatureNonce": str(uuid.uuid4()),
            "SignatureVersion": "1.0",
            "TemplateCode": tpl,
            "TemplateParam": json.dumps({"title": title, "content": content}, ensure_ascii=False),
            "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Version": "2017-05-25",
        }

        sorted_qs = "&".join(
            f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(str(v), safe='')}"
            for k, v in sorted(params.items())
        )
        string_to_sign = "GET&%2F&" + urllib.parse.quote(sorted_qs, safe="")
        sign_key = (key_secret + "&").encode("utf-8")
        signature = b64encode(
            hmac.new(sign_key, string_to_sign.encode("utf-8"), hashlib.sha1).digest()
        ).decode("utf-8")
        params["Signature"] = signature

        resp = requests.get("https://dysmsapi.aliyuncs.com/", params=params, timeout=10)
        data = resp.json()
        if data.get("Code") == "OK":
            return
        raise RuntimeError(f"阿里云短信: {data.get('Message', data)}")

    def _send_generic(self, config: dict, phone: str, message: str):
        """通用 HTTP POST API。"""
        api_url = config.get("api_url", "")
        api_key = config.get("api_key", "")
        if not api_url:
            raise ValueError("通用短信渠道缺少 api_url")

        payload = {
            "api_key": api_key,
            "phone": phone,
            "content": message,
        }
        resp = requests.post(api_url, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") in (0, "0", "OK", "ok", 200):
                return
            raise RuntimeError(f"短信发送失败: {data}")
        raise RuntimeError(f"短信 API 返回 {resp.status_code}")


CHANNEL_MAP = {
    "qq": QQChannel(),
    "sms": SMSChannel(),
}


def get_channel(channel_type: str) -> BaseChannel:
    ch = CHANNEL_MAP.get(channel_type)
    if not ch:
        raise ValueError(f"不支持的渠道类型: {channel_type}")
    return ch
