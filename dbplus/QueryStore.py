import re
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple, Union


class Query(NamedTuple):
    name: str
    comments: str
    sql: str
    floc: Optional[Tuple[Path, int]] = None


class SQLLoadException(Exception):
    """Raised when there is a problem loading SQL content from a file or directory"""

    pass


class SQLParseException(Exception):
    """Raised when there was a problem parsing the annotations in SQL"""

    pass


# identifies name definition comments
_QUERY_DEF = re.compile(r"--\s*name\s*:\s*")

# extract a valid query name followed by an optional operation spec
_NAME_OP = re.compile(r"^(\w+)(|\^|\$|!|<!|\*!|#)$")

# forbid numbers as first character
_BAD_PREFIX = re.compile(r"^\d")

# get SQL comment contents
_SQL_COMMENT = re.compile(r"\s*--\s*(.*)$")

# column-1 "-- version: <spec>" line; spaces/tabs only; captures spec sans trailing ws
_VERSION_LINE = re.compile(r"^--[ \t]+version[ \t]*:[ \t]*(\S.*?)[ \t]*$")

# version-spec operators, ordered longest-first so ">=" binds before ">"
_VERSION_OPS = (">=", "<=", "==", "!=", ">", "<")


def _parse_version(v: str) -> Tuple[int, ...]:
    try:
        return tuple(int(p) for p in v.strip().split("."))
    except ValueError:
        raise SQLParseException(f"invalid version: {v!r}")


def _parse_version_spec(spec: str) -> Tuple[str, Tuple[int, ...]]:
    s = spec.strip()
    for op in _VERSION_OPS:
        if s.startswith(op):
            return op, _parse_version(s[len(op):])
    return "==", _parse_version(s)


def _compare_versions(lhs: Tuple[int, ...], op: str, rhs: Tuple[int, ...]) -> bool:
    n = max(len(lhs), len(rhs))
    lhs = lhs + (0,) * (n - len(lhs))
    rhs = rhs + (0,) * (n - len(rhs))
    if op == "==":
        return lhs == rhs
    if op == "!=":
        return lhs != rhs
    if op == ">=":
        return lhs >= rhs
    if op == "<=":
        return lhs <= rhs
    if op == ">":
        return lhs > rhs
    if op == "<":
        return lhs < rhs
    raise SQLParseException(f"invalid version operator: {op!r}")


class QueryStore:
    def __init__(
        self,
        sql_path: Union[str, Path],
        ext: Tuple[str] = (".sql",),
        prefix: Optional[bool] = False,
        version: Optional[str] = None,
    ):
        self.query_store: Dict[str, Query] = {}
        self._candidates: Dict[str, List[Tuple[Optional[str], Query]]] = {}
        self._version_str = version
        self._version = _parse_version(version) if version is not None else None
        path = Path(sql_path)
        if not path.exists():
            raise SQLLoadException(f"File/Path does not exist: {path}")
        if path.is_file():
            self.load_query_data_from_file(path, prefix)
        elif path.is_dir():
            self.load_query_data_from_dir_path(path, ext, prefix)
        else:  # pragma: no cover
            raise SQLLoadException(
                f"{sql_path} is not valid for QueryStore, expecting file or path"
            )
        self._resolve_candidates()

    def __getattr__(self, name):
        try:
            return self.query_store[name]
        except KeyError:
            raise AttributeError(f"QueryStore has no query named '{name}'")

    def _make_query(
        self, query: str, floc: Optional[Tuple[Path, int]] = None, prefix: bool = False
    ) -> Tuple[Optional[str], Query]:
        raw = query.strip().splitlines()
        stripped = [line.strip() for line in raw]
        qname = self._get_name_op(stripped[0])
        if prefix:
            qname = floc[0].stem + "_" + qname

        version_spec: Optional[str] = None
        consumed = {0}
        for i in range(1, len(raw)):
            if not stripped[i].startswith("--"):
                break
            m = _VERSION_LINE.match(raw[i])
            if m:
                if version_spec is not None:
                    raise SQLParseException(
                        f"multiple -- version: lines for query {qname!r} in {floc}"
                    )
                version_spec = m.group(1).strip()
                _parse_version_spec(version_spec)
                consumed.add(i)

        remaining = [stripped[i] for i in range(len(stripped)) if i not in consumed]
        sql, doc = self._get_sql_doc(remaining)
        return version_spec, Query(qname, doc, sql, floc)

    def _get_name_op(self, text: str) -> str:
        qname_spec = text.replace("-", "_")
        nameop = _NAME_OP.match(qname_spec)
        if not nameop or _BAD_PREFIX.match(qname_spec):
            raise SQLParseException(
                f'invalid query name and operation spec: "{qname_spec}"'
            )
        qname, qop = nameop.group(1, 2)
        return qname

    def _get_sql_doc(self, lines: Sequence[str]) -> Tuple[str, str]:
        doc, sql = "", ""
        for line in lines:
            doc_match = _SQL_COMMENT.match(line)
            if doc_match:
                doc += doc_match.group(1) + "\n"
            else:
                sql += line + " "

        return sql.strip(), doc.rstrip()

    def _update_query_tree(self, version_spec: Optional[str], item: Query):
        candidates = self._candidates.setdefault(item.name, [])
        for existing_spec, existing_item in candidates:
            if existing_spec == version_spec:
                vlabel = (
                    f"version {version_spec!r}" if version_spec else "no version"
                )
                raise SQLLoadException(
                    f"duplicate {item.name} ({vlabel}) in {item.floc}, "
                    f"conflict with {existing_item.floc}"
                )
        candidates.append((version_spec, item))

    def _resolve_candidates(self):
        for name, cands in self._candidates.items():
            for spec, item in cands:
                if spec is None:
                    self.query_store[name] = item
                    break
                if self._version is None:
                    continue
                op, rhs = _parse_version_spec(spec)
                if _compare_versions(self._version, op, rhs):
                    self.query_store[name] = item
                    break

    def load_query_data_from_file(self, fname: Path, prefix: bool = False):
        qdefs = _QUERY_DEF.split(fname.read_text())
        lineno = 1 + qdefs[0].count("\n")
        # first item is anything before the first query definition, drop it!
        for qdef in qdefs[1:]:
            version_spec, item = self._make_query(qdef, (fname, lineno), prefix)
            self._update_query_tree(version_spec, item)
            lineno += qdef.count("\n")

    def load_query_data_from_dir_path(self, dir_path, ext=(".sql",), prefix=True):
        if not dir_path.is_dir():
            raise ValueError(f"The path {dir_path} must be a directory")

        def _recurse_load_query_data_tree(path, ext=(".sql",), prefix=False):
            for p in path.iterdir():
                if p.is_file():
                    if p.suffix not in ext:
                        continue
                    self.load_query_data_from_file(p, prefix)
                elif p.is_dir():
                    _recurse_load_query_data_tree(p, ext=ext, prefix=True)
                else:  # pragma: no cover
                    # This should be practically unreachable.
                    raise SQLLoadException(
                        f"The path must be a directory or file, got {p}"
                    )

        _recurse_load_query_data_tree(dir_path, ext=ext, prefix=False)
