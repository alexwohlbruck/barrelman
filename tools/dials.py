"""tools.dials — source-level introspection of the pipeline config dataclasses.

Every dial that moves the map lives as a field on a frozen/plain dataclass
(WaygraphConfig, SegmentConfig, MatchConfig, GateConfig, SolveConfig, plus
the module-level tag-filter/penalty tables in shapesnap.graph). This module
reads them straight from the SOURCE via ``ast`` so the manifest can never
drift from the code: the default is the literal in the file, the doc is the
field's own trailing inline comment (possibly spanning continuation lines)
or the block comment immediately above it.

Grouped by pipeline stage so the manifest reads top-to-bottom in build order.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


# (module path, qualified class name, stage label). Order = build order.
CONFIG_TARGETS: list[tuple[str, str, str]] = [
    ("shapesnap/graph.py", "__module_tables__", "shapesnap (graph tag policy)"),
    ("shapesnap/match.py", "MatchConfig", "shapesnap (HMM matcher)"),
    ("shapesnap/gates.py", "GateConfig", "shapesnap (quality gates)"),
    ("linegraph/waygraph.py", "WaygraphConfig", "linegraph (corridors + bundling)"),
    ("lineorder/solve.py", "SolveConfig", "lineorder (slot ordering)"),
    ("segments/segment.py", "SegmentConfig", "segments (steady/transition + bands)"),
]

# module-level constants in shapesnap/graph.py that are true dials (tag
# filters + penalty tables) — not a dataclass, but they move matching.
GRAPH_TABLE_NAMES = (
    "RAIL_INCLUDE", "RAIL_EXCLUDE", "RAIL_SERVICE_PENALTY",
    "RAIL_SERVICE_DEFAULT", "RAIL_USAGE_PENALTY", "NON_REGULAR_SERVICE_VALUES",
    "NON_REGULAR_USAGE_VALUES",
)


@dataclass
class Dial:
    name: str
    type: str
    default: str
    doc: str


def _default_src(node: ast.AST | None, src_lines: list[str]) -> str:
    """The default value's SOURCE text (so dicts/lambdas read verbatim)."""
    if node is None:
        return ""
    try:
        return ast.get_source_segment("\n".join(src_lines), node) or ""
    except Exception:
        return ""


def _inline_comment(src_lines: list[str], lineno_1: int, end_1: int) -> str:
    """Trailing ``# ...`` comment(s) on a field's own line(s).

    A field default that spans multiple physical lines (a wrapped dict, a
    lambda) can carry the comment on any of those lines; and a field
    documented by a comment continuing on the LINES BELOW (the WaygraphConfig
    style, where a short field is followed by an indented ``#`` block at the
    same or deeper indent) is stitched on too.
    """
    parts: list[str] = []
    for i in range(lineno_1 - 1, end_1):
        if i >= len(src_lines):
            break
        c = _strip_comment(src_lines[i])
        if c:
            parts.append(c)
    # continuation block: subsequent pure-comment lines indented under the
    # field (the corridor/segment configs write multi-line rationale this way).
    # Stops at a section divider (``# ── ... ──``) — that heads the NEXT group,
    # not this field's continuation — so a divider never leaks into a doc.
    if not parts:
        return ""  # a field with no OWN inline comment is documented by the
        #            block scan above it, not by lines below (those belong to
        #            the following field)
    j = end_1
    while j < len(src_lines):
        stripped = src_lines[j].strip()
        if not stripped.startswith("#") or _is_divider(stripped):
            break
        parts.append(stripped.lstrip("#").strip())
        j += 1
    return " ".join(p for p in parts if p)


def _is_divider(comment_line: str) -> bool:
    """A ``# ── section ──`` divider (box-drawing rule), not real prose."""
    body = comment_line.lstrip("#").strip()
    return bool(body) and body.count("─") >= 2


def _strip_comment(line: str) -> str:
    """The comment text after a ``#`` that is NOT inside a string literal."""
    in_s: str | None = None
    for k, ch in enumerate(line):
        if in_s:
            if ch == in_s and line[k - 1] != "\\":
                in_s = None
        elif ch in "\"'":
            in_s = ch
        elif ch == "#":
            return line[k + 1:].strip()
    return ""


def _block_comment_above(src_lines: list[str], lineno_1: int) -> str:
    """Contiguous ``# ...`` lines immediately above a field (block style)."""
    out: list[str] = []
    i = lineno_1 - 2  # 0-based line just above the field
    while i >= 0:
        s = src_lines[i].strip()
        if s.startswith("#"):
            out.append(s.lstrip("#").strip())
            i -= 1
        else:
            break
    out.reverse()
    return " ".join(out)


def _fields_from_classdef(cls: ast.ClassDef, src_lines: list[str]) -> list[Dial]:
    dials: list[Dial] = []
    for stmt in cls.body:
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(
                stmt.target, ast.Name):
            continue
        name = stmt.target.id
        ann = ast.get_source_segment("\n".join(src_lines), stmt.annotation) \
            if stmt.annotation is not None else ""
        default = _default_src(stmt.value, src_lines)
        end = getattr(stmt, "end_lineno", stmt.lineno)
        doc = _inline_comment(src_lines, stmt.lineno, end)
        if not doc:
            doc = _block_comment_above(src_lines, stmt.lineno)
        dials.append(Dial(name=name, type=ann or "", default=default, doc=doc))
    return dials


def _module_table_dials(tree: ast.Module, src_lines: list[str]) -> list[Dial]:
    """Module-level assignments that are dials (tag filters + penalties)."""
    dials: list[Dial] = []
    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign):
            continue
        if len(stmt.targets) != 1 or not isinstance(stmt.targets[0], ast.Name):
            continue
        name = stmt.targets[0].id
        if name not in GRAPH_TABLE_NAMES:
            continue
        default = _default_src(stmt.value, src_lines)
        end = getattr(stmt, "end_lineno", stmt.lineno)
        doc = _inline_comment(src_lines, stmt.lineno, end)
        if not doc:
            doc = _block_comment_above(src_lines, stmt.lineno)
        dials.append(Dial(name=name, type="table", default=default, doc=doc))
    return dials


def collect() -> list[tuple[str, str, list[Dial]]]:
    """[(stage_label, class_or_module_name, [Dial])] in build order."""
    out: list[tuple[str, str, list[Dial]]] = []
    for rel, clsname, stage in CONFIG_TARGETS:
        path = REPO_ROOT / rel
        if not path.exists():
            continue
        src = path.read_text()
        src_lines = src.splitlines()
        tree = ast.parse(src)
        if clsname == "__module_tables__":
            dials = _module_table_dials(tree, src_lines)
            out.append((stage, "shapesnap.graph tables", dials))
            continue
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == clsname:
                dials = _fields_from_classdef(node, src_lines)
                out.append((stage, clsname, dials))
                break
    return out
