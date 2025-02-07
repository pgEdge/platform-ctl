from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Union

from ace_auth import ConnectionPool

"""
Use a dataclass to store the raw and processed inputs from the user
"""


@dataclass
class Task:
    task_id: str = None
    task_type: str = None
    task_status: str = None
    task_context: str = None
    started_at: datetime = None
    finished_at: datetime = None
    time_taken: float = None


@dataclass
class DerivedFields:
    cluster_nodes: list = None
    l_schema: str = None
    l_table: str = None
    key: str = None
    cols: list = None
    conn_params: list = None
    database: str = None
    node_list: list = None
    host_map: dict = None
    table_list: list = None
    col_types: dict = None


@dataclass
class TableDiffTask:
    # Unprocessed fields
    _table_name: str  # Required
    _dbname: str
    _nodes: str

    # User-specified, validated fields
    cluster_name: str  # Required
    block_rows: int
    max_cpu_ratio: float
    output: str
    batch_size: int
    table_filter: str
    quiet_mode: bool

    # For table-diff, the diff_file_path is
    # obtained after the run of table-diff,
    # and is not mandatory
    diff_file_path: str = None

    invoke_method: str = "cli"

    # Client role from certificate CN when invoked via API
    client_role: str = None

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    diff_summary: dict = field(default_factory=dict)

    # If we're invoking table-diff from repset-diff,
    # we don't need to update the database with the
    # status of each table-diff task (for now)
    skip_db_update: bool = False

    scheduler: Task = field(default_factory=Task)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)


@dataclass
class TableRepairTask:
    # Unprocessed fields
    _table_name: str
    _dbname: str

    # Mandatory fields
    cluster_name: str

    # For table-repair, the diff_file_path is
    # mandatory, as it is used to repair the
    # tables
    diff_file_path: str
    source_of_truth: str

    # Optional fields, but non-default since the handler method will fill in the
    # default values
    quiet_mode: bool
    dry_run: bool
    generate_report: bool
    insert_only: bool
    upsert_only: bool
    fix_nulls: bool
    fire_triggers: bool

    bidirectional: bool

    invoke_method: str = "cli"

    # Client role from certificate CN when invoked via API
    client_role: str = None

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    # Task-specific parameters
    scheduler: Task = field(default_factory=Task)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)


@dataclass
class RepsetDiffTask:
    # Unprocessed fields
    _dbname: str
    _nodes: str

    # Mandatory fields
    cluster_name: str
    repset_name: str

    # Optional fields
    # Non-default members since the handler method will fill in the
    # default values
    block_rows: int
    max_cpu_ratio: float
    output: str
    batch_size: int
    quiet_mode: bool

    skip_tables: Union[str, list]
    skip_file: str

    invoke_method: str = "cli"

    # Client role from certificate CN when invoked via API
    client_role: str = None

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    # Task-specific parameters
    scheduler: Task = field(default_factory=Task)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)


@dataclass
class SpockDiffTask:
    # Mandatory fields
    cluster_name: str

    # Optional fields
    # Non-default members since the handler method will fill in the
    # default values
    _nodes: str
    _dbname: str
    quiet_mode: bool

    invoke_method: str = "cli"

    # Client role from certificate CN when invoked via API
    client_role: str = None

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    # Task-specific parameters
    scheduler: Task = field(default_factory=Task)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)


@dataclass
class SchemaDiffTask:
    # Mandatory fields
    cluster_name: str
    schema_name: str

    # Optional fields
    # Non-default members since the handler method will fill in the
    # default values
    _nodes: str
    _dbname: str
    quiet_mode: bool

    ddl_only: bool

    skip_tables: Union[str, list]
    skip_file: str

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    # Client role from certificate CN when invoked via API
    client_role: str = None

    invoke_method: str = "cli"

    # Task-specific parameters
    scheduler: Task = field(default_factory=Task)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)


# TODO: Handle connection pool for auto-repair tasks!!
@dataclass
class ExceptionLogEntry:
    remote_origin: int
    remote_commit_ts: datetime
    command_counter: int
    remote_xid: int
    local_origin: int
    local_commit_ts: datetime
    table_schema: str
    table_name: str
    operation: str
    local_tup: defaultdict
    remote_old_tup: defaultdict
    remote_new_tup: defaultdict
    ddl_statement: str
    ddl_user: str
    error_message: str
    retry_errored_at: datetime


@dataclass
class AutoRepairTask:
    cluster_name: str
    dbname: str
    poll_frequency: str
    repair_frequency: str

    exp_log_entries: list[ExceptionLogEntry] = field(default_factory=list)

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)

    # Task-specific parameters
    scheduler: Task = field(default_factory=Task)


@dataclass
class MerkleTreeTask:
    # Can be one of : build, update, rebalance
    mode: str
    cluster_name: str
    _table_name: str
    _dbname: str
    _nodes: str

    analyse: bool
    rebalance: bool
    block_rows: int
    max_cpu_ratio: float
    quiet_mode: bool

    invoke_method: str = "cli"

    # Client role from certificate CN when invoked via API
    client_role: str = None

    connection_pool: ConnectionPool = field(default_factory=ConnectionPool)

    scheduler: Task = field(default_factory=Task)

    # Derived fields
    fields: DerivedFields = field(default_factory=DerivedFields)
    
