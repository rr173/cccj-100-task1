"""确定性故障注入点。

三个注入点分别对应装载流程的关键位置：

``before_snapshot_publish``
    全部分片已校验并暂存、但还没把暂存行发布成主表快照之前。
``before_txn_commit``
    事务的行变更已经在同一个 SQLite 事务内执行、检查点更新之前
    （即主表写入对外部尚不可见的时刻）。
``after_txn_commit_before_return``
    SQLite 事务已经成功提交、装载器尚未向调用方返回成功之前。

注入方式是 ``os._exit(42)``：跳过所有 Python 层清理与缓冲刷新，
模拟真实的进程崩溃。每个注入点在一次进程生命周期内只触发一次，
重复调用同一注入点不会再次崩溃，保证“继续消费”路径可演示。

生产环境不传 fault 参数即可，注入器永远为空操作。
"""

from __future__ import annotations

import os
import sys

FAULT_BEFORE_SNAPSHOT_PUBLISH = "before_snapshot_publish"
FAULT_BEFORE_TXN_COMMIT = "before_txn_commit"
FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN = "after_txn_commit_before_return"

ALL_FAULTS = frozenset(
    {
        FAULT_BEFORE_SNAPSHOT_PUBLISH,
        FAULT_BEFORE_TXN_COMMIT,
        FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN,
    }
)

CRASH_EXIT_CODE = 42


class FaultInjector:
    def __init__(self, fault: str | None = None) -> None:
        if fault is not None and fault not in ALL_FAULTS:
            raise ValueError(f"未知故障注入点: {fault}，允许: {sorted(ALL_FAULTS)}")
        self._fault = fault
        self._fired = False

    @property
    def fault(self) -> str | None:
        return self._fault

    def hit(self, point: str) -> None:
        """到达注入点；若命中则以确定性方式终止本进程。"""
        if self._fired or point != self._fault:
            return
        self._fired = True
        sys.stderr.write(
            f"[fault-inject] 命中注入点 {point!r}，进程以退出码 {CRASH_EXIT_CODE} 终止\n"
        )
        sys.stderr.flush()
        # os._exit 不执行 finally / atexit / 缓冲刷新，最接近真实崩溃。
        os._exit(CRASH_EXIT_CODE)
