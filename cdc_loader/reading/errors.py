"""读数事实仓统一错误类型（退出码与装载器/归并层错误族对齐但互不依赖）。"""

from __future__ import annotations


class ReadingError(Exception):
    """所有读数仓错误的基类。"""

    exit_code = 1


class ReadingProtocolError(ReadingError):
    """读数行违反字段格式协议（缺字段、类型不符、时间/十进制非法等）。"""

    exit_code = 2


class EditionConflict(ReadingError):
    """同一观测身份 (origin, reading_key, edition) 已登记且内容不同。

    重复投递内容完全一致的旧稿件是幂等重传；同一稿件换了任何内容字段
    都是数据正确性红线，整批拒收并保持事实仓现状。
    """

    exit_code = 3


class CaliberNotFound(ReadingError):
    """统计口径引用了不存在的归并层分组版本，或没有可用的截止批次。"""

    exit_code = 7
