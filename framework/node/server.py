# -*- coding: utf-8 -*-
"""
node_server：一个数据库节点的 HTTP 服务进程。
- /op       业务操作（冻结/扣减/返还/解冻/事务读写…），由节点内"单库本地事务"完成
- /health   健康检查（框架/其它节点靠它探活）
- /admin    运维接口：分片宕机模拟、种子数据、触发重启自检
用户节点、店家节点、事务节点三份独立进程，互不通信，仅各自持有自己的 JSON 分片。
"""
import json
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ..errors import BusinessFailure, DbUnavailable
from ..proto import (
    ROUTE_OP, ROUTE_HEALTH, ROUTE_ADMIN, ADMIN_OUTAGE, ADMIN_RESTART_SELFCHECK,
    ADMIN_FAIL_COMMIT,
)
from .logic import build_logic
from .self_check import SelfCheckPlugin


class _Handler(BaseHTTPRequestHandler):
    server_version = "BrainSplitNode/1.0"

    def log_message(self, *a):
        pass  # 安静模式

    def _reply(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_POST(self):
        node = self.server.node
        try:
            payload = self._read_json()
        except Exception as e:
            return self._reply({"ok": False, "error": {"type": "internal", "message": f"bad json: {e}"}}, 400)

        try:
            if self.path == ROUTE_OP:
                data = node.logic.dispatch(payload.get("cmd"), payload.get("payload") or {})
                return self._reply({"ok": True, "data": data})
            if self.path == ROUTE_ADMIN:
                return self._reply({"ok": True, "data": node.handle_admin(payload)})
            return self._reply({"ok": False, "error": {"type": "internal", "message": f"unknown route {self.path}"}}, 404)
        except BusinessFailure as e:
            return self._reply({"ok": False, "error": {"type": "business", "message": str(e)}})
        except DbUnavailable as e:
            return self._reply({"ok": False, "error": {"type": "unavailable", "message": str(e)}})
        except KeyError as e:
            return self._reply({"ok": False, "error": {"type": "business", "message": f"未知命令: {e}"}})
        except Exception as e:
            traceback.print_exc()
            return self._reply({"ok": False, "error": {"type": "internal", "message": str(e)}}, 500)

    def do_GET(self):
        node = self.server.node
        if self.path == ROUTE_HEALTH:
            return self._reply({"ok": True, "data": node.health()})
        return self._reply({"ok": False, "error": {"type": "internal", "message": "not found"}}, 404)


class NodeServer:
    def __init__(self, role, port, data_dir, txn_url=None,
                 shards=None, replicas=None, self_check=True):
        self.role = role
        self.port = port
        self.logic = build_logic(role, data_dir, shards=shards, replicas=replicas)
        self.txn_client = None
        self.self_check_plugin = None
        if self_check and role in ("user", "shop") and txn_url:
            from ..client.typed_clients import TxnClient
            self.txn_client = TxnClient(txn_url)
            self.self_check_plugin = SelfCheckPlugin(self.logic, self.txn_client)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
        self.httpd.node = self

    # ---------- admin ----------
    def handle_admin(self, payload):
        action = payload.get("action")
        body = payload.get("payload") or {}
        if action == ADMIN_OUTAGE:
            shard = body.get("shard")
            if body.get("clear"):
                self.logic.clear_outage(shard)
                return {"shard": shard, "state": "up"}
            self.logic.mark_outage(shard, body.get("seconds", 5))
            return {"shard": shard, "state": "down", "seconds": body.get("seconds", 5)}
        if action == ADMIN_RESTART_SELFCHECK:
            return self.run_self_check()
        if action == ADMIN_FAIL_COMMIT:
            mode = body.get("mode", "unavailable")
            self.logic.fail_commit_once = mode
            return {"armed": mode}
        raise KeyError(f"未知 admin action: {action}")

    def health(self):
        return {"role": self.role, "status": "up", "port": self.port,
                "outage": self.logic.outage_state()}

    # ---------- 启动自检 ----------
    def run_self_check(self):
        if self.self_check_plugin is None:
            return {"note": f"{self.role} 节点无自检插件"}
        return self.self_check_plugin.run_once()

    def start(self):
        if self.self_check_plugin is not None:
            def _boot():
                import time
                time.sleep(0.2)  # 等事务中心就绪
                try:
                    self.run_self_check()
                except Exception as e:
                    print(f"[{self.role}] 启动自检失败: {e}", flush=True)
            threading.Thread(target=_boot, daemon=True).start()
        print(f"[{self.role}] node server listening on 127.0.0.1:{self.port}", flush=True)
        self.httpd.serve_forever()


def run_server(role, port, data_dir, txn_url=None, shards=None, replicas=None):
    NodeServer(role, port, data_dir, txn_url=txn_url,
               shards=shards, replicas=replicas).start()
