"""装载器统一错误类型。"""

from __future__ import annotations


class CDCError(Exception):
    """所有装载器错误的基类。"""

    exit_code = 1


class ProtocolError(CDCError):
    """输入数据违反项目定义的快照/事件协议。"""

    exit_code = 2


class PayloadConflict(CDCError):
    """同一事件身份（txn_id + seq）出现了不同载荷。"""

    exit_code = 3


class SnapshotIncomplete(CDCError):
    """快照未完整发布（分片缺失或摘要不符），暂不允许追日志。"""

    exit_code = 4


class FaultInjected(CDCError):
    """确定性故障注入触发（测试框架可据此判断注入点被命中）。"""

    exit_code = 5
