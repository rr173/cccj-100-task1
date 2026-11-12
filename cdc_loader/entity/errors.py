"""实体归并层统一错误类型（退出码与装载器错误族对齐但互不依赖）。"""

from __future__ import annotations


class EntityError(Exception):
    """所有实体归并层错误的基类。"""

    exit_code = 1


class RelationProtocolError(EntityError):
    """对照资料行违反项目定义的字段格式协议。"""

    exit_code = 2


class RevisionConflict(EntityError):
    """已登记的修订（任意旧版本，不只最新版）出现了不同内容。

    重复投递内容完全一致的旧修订是幂等重传；只要有一个字段被改写，
    即视为数据正确性红线，整批拒绝并保持现状。
    """

    exit_code = 3


class UpstreamNotReady(EntityError):
    """上游设备主数据不可用：装载器快照未发布，或尚未执行 refresh。"""

    exit_code = 4


class ConstraintConflict(EntityError):
    """本批投递使“必须不同”约束落在同一实体内，整批拒绝。

    ``report`` 携带结构化冲突报告：每条冲突约束 + 连接两端的
    确定性关系路径，供调用方原样输出。
    """

    exit_code = 5

    def __init__(self, message: str, report: dict | None = None) -> None:
        super().__init__(message)
        self.report = report or {}


class VersionNotFound(EntityError):
    """请求的决策版本不存在（或没有前驱版本可比较）。"""

    exit_code = 7
