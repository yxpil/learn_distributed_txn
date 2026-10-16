# -*- coding: utf-8 -*-
"""事务状态机：状态枚举、流转守卫、终态判定。"""

from ..proto import (
    INIT, FROZEN, ROLLBACK, ROLLED_BACK, SUCCESS, MANUAL_REVIEW, TERMINAL,
)

# 允许的流转（终态与 MANUAL_REVIEW 不再出边）
_ALLOWED = {
    INIT: {FROZEN, ROLLBACK, MANUAL_REVIEW},
    FROZEN: {SUCCESS, ROLLBACK, MANUAL_REVIEW},
    ROLLBACK: {ROLLED_BACK, MANUAL_REVIEW},
}


class IllegalStateTransition(Exception):
    pass


def is_terminal(state):
    return state in TERMINAL


def can_transition(src, dst):
    if src == dst:            # 幂等重放，允许原地
        return True
    if src not in _ALLOWED:
        return False
    if dst == MANUAL_REVIEW and src not in TERMINAL:
        return True           # 任何非终态都可因重试耗尽进入人工复核
    return dst in _ALLOWED[src]


def validate_transition(src, dst):
    if not can_transition(src, dst):
        raise IllegalStateTransition(f"非法状态流转: {src} -> {dst}")
    return True


def describe():
    lines = ["事务状态机流转规则："]
    for src, dsts in _ALLOWED.items():
        lines.append(f"  {src:<12} -> {', '.join(sorted(dsts))}")
    lines.append("  任意非终态    -> MANUAL_REVIEW（重试耗尽，人工复核）")
    return "\n".join(lines)
