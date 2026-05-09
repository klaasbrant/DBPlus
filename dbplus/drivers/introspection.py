"""Driver-agnostic introspection contract.

Drivers that implement this Protocol expose a uniform schema inspection API
that the MCP server (and any other consumer) can use without knowing about
the underlying backend. Dataclasses are plain Python types so they can be
serialized to JSON directly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@dataclass
class SchemaInfo:
    name: str
    owner: Optional[str] = None
    remarks: Optional[str] = None


@dataclass
class TableInfo:
    schema: str
    name: str
    type: str  # "TABLE", "VIEW", "ALIAS", "MQT", "NICKNAME", ...
    remarks: Optional[str] = None


@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    ordinal: int
    length: Optional[int] = None
    scale: Optional[int] = None
    default: Optional[str] = None
    remarks: Optional[str] = None


@dataclass
class TableDetail:
    schema: str
    name: str
    type: str
    columns: List[ColumnInfo]
    primary_key: List[str] = field(default_factory=list)
    remarks: Optional[str] = None


@dataclass
class IndexColumn:
    name: str
    ordinal: int
    ordering: str  # "ASC", "DESC", "INCLUDE"


@dataclass
class IndexInfo:
    name: str
    schema: str
    table_schema: str
    table_name: str
    unique: bool
    columns: List[IndexColumn]
    type: Optional[str] = None


@dataclass
class ForeignKeyInfo:
    name: str
    schema: str
    table: str
    columns: List[str]
    ref_schema: str
    ref_table: str
    ref_columns: List[str]
    on_delete: Optional[str] = None
    on_update: Optional[str] = None


@dataclass
class ViewInfo:
    schema: str
    name: str
    definition: str
    readonly: bool = False


@dataclass
class RoutineInfo:
    schema: str
    name: str
    type: str  # "PROCEDURE", "FUNCTION"
    language: Optional[str] = None
    remarks: Optional[str] = None


@dataclass
class TriggerInfo:
    schema: str
    name: str
    table_schema: str
    table_name: str
    event: str  # "INSERT", "UPDATE", "DELETE"
    timing: str  # "BEFORE", "AFTER", "INSTEAD OF"
    definition: Optional[str] = None


@dataclass
class StatsInfo:
    schema: str
    name: str
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    last_analyzed: Optional[str] = None


@dataclass
class ServerInfo:
    product: str
    version: str
    platform: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryValidation:
    """Result of :meth:`Introspector.validate_query`."""

    valid: bool
    error: Optional[str] = None


@dataclass
class SearchResult:
    """One catalog object matching :meth:`Introspector.search_objects`."""

    kind: str              # "TABLE", "VIEW", "COLUMN", "PROCEDURE"
    schema: str
    name: str
    table: Optional[str] = None    # parent table name (COLUMN kind only)
    remarks: Optional[str] = None


@runtime_checkable
class Introspector(Protocol):
    def list_schemas(self) -> List[SchemaInfo]: ...

    def list_tables(
        self, schema: Optional[str] = None, kind: str = "TABLE"
    ) -> List[TableInfo]: ...

    def describe_table(self, schema: str, table: str) -> TableDetail: ...

    def list_indexes(self, schema: str, table: str) -> List[IndexInfo]: ...

    def list_foreign_keys(
        self, schema: str, table: str
    ) -> List[ForeignKeyInfo]: ...

    def get_view(self, schema: str, view: str) -> ViewInfo: ...

    def list_procedures(
        self, schema: Optional[str] = None
    ) -> List[RoutineInfo]: ...

    def list_triggers(
        self, schema: Optional[str] = None
    ) -> List[TriggerInfo]: ...

    def get_table_stats(self, schema: str, table: str) -> StatsInfo: ...

    def list_table_stats(self, schema: str) -> List[StatsInfo]: ...

    def sample_rows(
        self, schema: str, table: str, n: int = 5
    ) -> List[Dict[str, Any]]: ...

    def validate_query(self, sql: str) -> QueryValidation: ...

    def describe_query(self, sql: str) -> List[ColumnInfo]: ...

    def search_objects(
        self,
        pattern: str,
        kinds: Optional[List[str]] = None,
    ) -> List[SearchResult]: ...

    def server_info(self) -> ServerInfo: ...
