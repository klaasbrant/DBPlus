from pathlib import Path
from typing import Dict, List, Optional, Tuple, Sequence, Union, NamedTuple
import re
from enum import Enum


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


class QueryStore(object):
    def __init__(
        self,
        sql_path: Union[str, Path],
        ext: Tuple[str] = (".sql",),
        prefix: Optional[bool] = False,
    ):
        self.query_store = {}
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

    def __getattr__(self, name):
        return self.query_store[name]

    def _make_query(
        self, query: str, floc: Optional[Tuple[Path, int]] = None, prefix: bool = False
    ) -> Query:
        lines = [line.strip() for line in query.strip().splitlines()]
        qname = self._get_name_op(lines[0])
        if prefix:
            qname = floc[0].stem + "_" + qname
        sql, doc = self._get_sql_doc(lines[1:])
        return Query(qname, doc, sql, floc)

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

    def _update_query_tree(self, item: Query):
        if item.name not in self.query_store:
            self.query_store[item.name] = item
        else:
            raise SQLLoadException(
                f"duplicate {item.name} in {item.floc}, conflict with {self.query_store[item.name].floc} "
            )

    def load_query_data_from_file(self, fname: Path, prefix: bool = False):
        qdefs = _QUERY_DEF.split(fname.read_text())
        lineno = 1 + qdefs[0].count("\n")
        # first item is anything before the first query definition, drop it!
        for qdef in qdefs[1:]:
            self._update_query_tree(self._make_query(qdef, (fname, lineno), prefix))
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
