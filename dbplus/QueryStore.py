from pathlib import Path
from typing import Dict, List, Optional, Tuple, Sequence, Union, NamedTuple
import re
from enum import Enum


class Query(NamedTuple):
    name: str
    comments: str
    sql: str
    floc: Optional[Tuple[Path, int]] = None


# Can't make this a recursive type in terms of itself
# QueryDataTree = Dict[str, Union[Query, 'QueryDataTree']]
QueryDataTree = Dict[str, Union[Query, Dict]]


class SQLLoadException(Exception):
    """Raised when there is a problem loading SQL content from a file or directory"""

    pass


class SQLParseException(Exception):
    """Raised when there was a problem parsing the aiosql comment annotations in SQL"""

    pass


# identifies name definition comments
_QUERY_DEF = re.compile(r"--\s*name\s*:\s*")

# extract a valid query name followed by an optional operation spec
_NAME_OP = re.compile(r"^(\w+)(|\^|\$|!|<!|\*!|#)$")

# forbid numbers as first character
_BAD_PREFIX = re.compile(r"^\d")

# get SQL comment contents
_SQL_COMMENT = re.compile(r"\s*--\s*(.*)$")


class QueryStore(object):
    def __init__(self, sql_path: Union[str, Path], ext: Tuple[str] = (".sql",)):
        path = Path(sql_path)
        if not path.exists():
            raise SQLLoadException(f"File does not exist: {path}")
        if path.is_file():
            query_data = self.load_query_data_from_file(path)
            print(query_data)
        elif path.is_dir():
            self.query_store = self.load_query_data_from_dir_path(path, ext=ext)
        else:  # pragma: no cover
            raise SQLLoadException(
                f"The sql_path must be a directory or file, got {sql_path}"
            )
    
    def __getattr__(self, name):
        return self.query_store[name]

    def _make_query(
        self, query: str, ns_parts: List[str], floc: Optional[Tuple[Path, int]] = None
    ) -> Query:
        lines = [line.strip() for line in query.strip().splitlines()]
        qname = self._get_name_op(lines[0])
        sql, doc = self._get_sql_doc(lines[1:])
        query_fqn = ".".join(ns_parts + [qname])
        return Query(query_fqn, doc, sql, floc)

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

    def load_query_data_from_sql(
        self, sql: str, ns_parts: List[str] = [], fname: Optional[Path] = None
    ) -> List[Query]:
        qdefs = _QUERY_DEF.split(sql)
        lineno = 1 + qdefs[0].count("\n")
        data = []
        # first item is anything before the first query definition, drop it!
        for qdef in qdefs[1:]:
            data.append(
                self._make_query(qdef, ns_parts, (fname, lineno) if fname else None)
            )
            lineno += qdef.count("\n")
        return data

    def load_query_data_from_file(
        self, path: Path, ns_parts: List[str] = []
    ) -> List[Query]:
        return self.load_query_data_from_sql(path.read_text(), ns_parts, path)

    def load_query_data_from_dir_path(self, dir_path, ext=(".sql",)) -> QueryDataTree:
        if not dir_path.is_dir():
            raise ValueError(f"The path {dir_path} must be a directory")

        def _recurse_load_query_data_tree(path, ns_parts=[], ext=(".sql",)):
            query_data_tree = {}
            for p in path.iterdir():
                if p.is_file():
                    if p.suffix not in ext:
                        continue
                    for query_datum in self.load_query_data_from_file(p, ns_parts):
                        query_data_tree[query_datum.name] = query_datum
                elif p.is_dir():
                    query_data_tree[p.name] = _recurse_load_query_data_tree(
                        p, ns_parts + [p.name], ext=ext
                    )
                else:  # pragma: no cover
                    # This should be practically unreachable.
                    raise SQLLoadException(
                        f"The path must be a directory or file, got {p}"
                    )
            return query_data_tree

        return _recurse_load_query_data_tree(dir_path, ext=ext)