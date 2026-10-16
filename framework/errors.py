# -*- coding: utf-8 -*-
"""框架统一异常：节点逻辑与客户端共享，避免 isinstance 错位。"""


class BusinessFailure(Exception):
    """业务性失败：余额/库存不足、标记缺失等。重试无意义，直接走补偿。"""


class DbUnavailable(Exception):
    """瞬时故障：节点/分片不可达（网络隔离、宕机、进程退出）。可重试。"""


class IllegalState(Exception):
    """状态机非法流转。"""
