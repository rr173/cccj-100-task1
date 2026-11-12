"""采样覆盖审计层统一错误类型（退出码与既有各层错误族对齐但互不依赖）。"""

from __future__ import annotations


class CoverageError(Exception):
    """所有覆盖审计错误的基类。"""

    exit_code = 1


class CoverageProtocolError(CoverageError):
    """契约/豁免行违反字段格式协议（缺字段、类型不符、时间非法等）。"""

    exit_code = 2


class DraftConflict(CoverageError):
    """同一稿号 (contract_no, draft_no) 已登记且内容不同。

    契约稿件不可变：重复投递内容完全一致是幂等重传；同一稿号换了任何
    内容字段（哪怕该稿已被更新稿取代或停用）都是数据正确性红线，整批
    拒收并保持现状。
    """

    exit_code = 3


class CaliberNotFound(CoverageError):
    """审计口径引用了不存在的批次/成员版本，或口径不合法。"""

    exit_code = 7


class AuditStale(CoverageError):
    """审计处于 building 残档（上次进程在对外确认前退出），不可对外读取。"""

    exit_code = 8
