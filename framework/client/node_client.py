# -*- coding: utf-8 -*-
"""NodeClient：框架 -> 单个 node 服务器的 HTTP JSON 客户端。
连接失败/超时视为 DbUnavailable；服务器返回业务错误视为 BusinessFailure。"""
import json
import socket
import urllib.error
import urllib.request

from ..errors import BusinessFailure, DbUnavailable
from ..proto import ROUTE_OP, ROUTE_HEALTH


class NodeClient:
    def __init__(self, base_url, name="node", timeout=3.0):
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.timeout = timeout

    def post(self, path, payload, timeout=None):
        url = self.base_url + path
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
            raise DbUnavailable(f"{self.name} 不可达: {e}")
        if not body.get("ok"):
            err = body.get("error") or {}
            etype = err.get("type")
            msg = err.get("message", "unknown")
            if etype == "business":
                raise BusinessFailure(msg)
            if etype == "unavailable":
                raise DbUnavailable(msg)
            raise RuntimeError(f"{self.name} 内部错误: {msg}")
        return body.get("data")

    def health(self):
        try:
            with urllib.request.urlopen(
                    self.base_url + ROUTE_HEALTH, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def call(self, cmd, payload=None):
        return self.post(ROUTE_OP, {"cmd": cmd, "payload": payload or {}})

    def admin(self, action, payload=None):
        from ..proto import ROUTE_ADMIN
        return self.post(ROUTE_ADMIN, {"action": action, "payload": payload or {}})
