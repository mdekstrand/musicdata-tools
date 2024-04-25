import re
from pathlib import Path
from typing import NamedTuple

from yaml import safe_load

_YAML_RE = re.compile(r"^--:\s+(.*)")


class SqlData(NamedTuple):
    meta: dict
    sql: str


def parse_sqlinfo(fn: Path) -> SqlData:
    """
    Parse metadata out of an SQL script.
    """
    text = fn.read_text()
    yaml_lines = []
    sql_lines = []
    for line in text.splitlines():
        m = _YAML_RE.match(line)
        if m:
            yaml_lines.append(m[1])
        else:
            sql_lines.append(line)

    yaml = "\n".join(yaml_lines)
    sql = "\n".join(sql_lines)
    if yaml.strip():
        meta = safe_load(yaml)
    else:
        meta = {}
    return meta, sql
