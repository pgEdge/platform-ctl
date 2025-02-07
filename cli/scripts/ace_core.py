import json
from math import ceil
import os
from typing import Union
from datetime import datetime
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor

import psycopg
from multiprocessing import Manager, cpu_count
from mpire import WorkerPool
from mpire.utils import make_single_arguments
from ordered_set import OrderedSet
from psycopg import sql
from psycopg.rows import dict_row, class_row
from dateutil import parser

from tqdm import tqdm
import ace
import ace_db
import ace_html_reporter
from ace_sql import GET_PKEY_OFFSETS
import cluster
import util
import ace_config as config
from ace_data_models import (
    AutoRepairTask,
    RepsetDiffTask,
    SchemaDiffTask,
    SpockDiffTask,
    TableDiffTask,
    TableRepairTask,
    ExceptionLogEntry,
)

from ace_exceptions import AceException
import ace_auth as auth


def run_query(worker_state, host, query):
    cur = worker_state[host]
    cur.execute(query)
    results = cur.fetchall()
    return results


def init_conn_pool(shared_objects, worker_state):

    td_task = shared_objects["td_task"]

    for node in td_task.fields.cluster_nodes:
        try:
            _, conn = td_task.connection_pool.get_cluster_node_connection(
                node,
                client_role=(
                    td_task.client_role
                    if config.USE_CERT_AUTH and td_task.invoke_method == "api"
                    else None
                ),
            )
            worker_state[node["name"]] = conn.cursor()
        except auth.AuthenticationError as e:
            raise AceException(str(e))


# Ignore the type checker warning for shared_objects.
# It is unused in this function, but is required by the mpire library.
def close_conn_pool(shared_objects, worker_state):
    try:
        for host, cur in worker_state.items():
            conn = cur.connection
            cur.close()
            conn.close()
    except Exception as e:
        raise AceException(f"Error closing connection pool: {e}")


# Accepts list of pkeys and values and generates a where clause that in the form
# `(pkey1name, pkey2name ...) in ( (pkey1val1, pkey2val1 ...),
#                                 (pkey1val2, pkey2val2 ...) ... )`
def generate_where_clause(primary_keys, id_values):
    if len(primary_keys) == 1:
        # Single primary key
        id_values_list = ", ".join(repr(val) for val in id_values)
        # Wrap column name in double quotes to preserve case
        query = f'"{primary_keys[0]}" IN ({id_values_list})'

    else:
        # Composite primary key
        conditions = ", ".join(
            f"({', '.join(repr(val) for val in id_tuple)})" for id_tuple in id_values
        )
        # Wrap each column name in double quotes to preserve case
        key_columns = ", ".join(f'"{key}"' for key in primary_keys)
        query = f"({key_columns}) IN ({conditions})"

    return query


def create_result_dict(
    node_pair,
    pkey_range,
    status_code,
    status_message,
    errors=False,
    error_messages=None,
):
    return {
        "node_pair": node_pair,
        "pkey_range": pkey_range,
        "status_code": status_code,
        "status_message": status_message,
        "errors": errors,
        "error_messages": error_messages or [],
    }


def compare_checksums(shared_objects, worker_state, batches):
    p_key = shared_objects["p_key"]
    schema_name = shared_objects["schema_name"]
    table_name = shared_objects["table_name"]
    node_list = shared_objects["node_list"]
    cols = shared_objects["cols_list"]
    simple_primary_key = shared_objects["simple_primary_key"]
    mode = shared_objects["mode"]
    stop_event = shared_objects["stop_event"]

    worker_diffs = {}
    total_diffs = 0

    for batch in batches:
        if stop_event.is_set():
            return

        where_clause = str()
        where_clause_temp = list()

        if mode == "diff":
            pkey1, pkey2 = batch
            if simple_primary_key:
                if pkey1 is not None:
                    where_clause_temp.append(
                        sql.SQL("{p_key} >= {pkey1}").format(
                            p_key=sql.Identifier(p_key), pkey1=sql.Literal(pkey1[0])
                        )
                    )
                if pkey2 is not None:
                    where_clause_temp.append(
                        sql.SQL("{p_key} < {pkey2}").format(
                            p_key=sql.Identifier(p_key), pkey2=sql.Literal(pkey2[0])
                        )
                    )
            else:
                """
                This is a slightly more complicated case since we have to split up
                the primary key and compare them with split values of pkey1 and pkey2
                """

                if pkey1 is not None:
                    where_clause_temp.append(
                        sql.SQL("({p_key}) >= ({pkey1})").format(
                            p_key=sql.SQL(", ").join(
                                [
                                    sql.Identifier(col.strip())
                                    for col in p_key.split(",")
                                ]
                            ),
                            pkey1=sql.SQL(", ").join(
                                [sql.Literal(val) for val in pkey1]
                            ),
                        )
                    )

                if pkey2 is not None:
                    where_clause_temp.append(
                        sql.SQL("({p_key}) < ({pkey2})").format(
                            p_key=sql.SQL(", ").join(
                                [
                                    sql.Identifier(col.strip())
                                    for col in p_key.split(",")
                                ]
                            ),
                            pkey2=sql.SQL(", ").join(
                                [sql.Literal(val) for val in pkey2]
                            ),
                        )
                    )

            where_clause = sql.SQL(" AND ").join(where_clause_temp)

        elif mode == "rerun":
            keys = p_key.split(",")
            where_clause = sql.SQL(generate_where_clause(keys, batch))

        else:
            raise Exception(f"Mode {mode} not recognized in compare_checksums")

        if simple_primary_key:
            hash_sql = sql.SQL(
                """
                WITH block_rows AS (
                SELECT *
                FROM {schema}.{table}
                WHERE {where_clause}
                ),
                block_hash AS (
                    SELECT
                        digest(
                            COALESCE(
                                string_agg(
                                    concat_ws('|', {columns}),
                                    '|'
                                    ORDER BY {key}
                                ),
                                'EMPTY_BLOCK'
                            ),
                            'sha256'
                        ) as leaf_hash
                    FROM block_rows
                )
                SELECT encode(leaf_hash, 'hex') as leaf_hash
                FROM block_hash
                """
            ).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
                where_clause=where_clause,
                key=sql.Identifier(p_key),
                columns=sql.SQL(", ").join(sql.Identifier(col) for col in cols),
            )
        else:
            hash_sql = sql.SQL(
                """
                WITH block_rows AS (
                    SELECT *
                    FROM {schema}.{table}
                    WHERE {where_clause}
                ),
                block_hash AS (
                    SELECT
                        digest(
                            COALESCE(
                                string_agg(
                                    concat_ws('|', {columns}),
                                    '|'
                                    ORDER BY {key}
                                ),
                                'EMPTY_BLOCK'
                            ),
                            'sha256'
                        ) as leaf_hash
                    FROM block_rows
                )
                SELECT encode(leaf_hash, 'hex') as leaf_hash
                FROM block_hash
                """
            ).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
                where_clause=where_clause,
                key=sql.SQL(", ").join(
                    sql.Identifier(col.strip()) for col in p_key.split(",")
                ),
                columns=sql.SQL(", ").join(sql.Identifier(col) for col in cols),
            )

        block_sql = sql.SQL("SELECT * FROM {table_name} WHERE {where_clause}").format(
            table_name=sql.SQL("{}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            ),
            where_clause=where_clause,
        )

        for node_pair in combinations(node_list, 2):
            host1 = node_pair[0]
            host2 = node_pair[1]

            # Run the checksum query on both nodes in parallel
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(run_query, worker_state, host1, hash_sql),
                    executor.submit(run_query, worker_state, host2, hash_sql),
                ]
                results = [f.result() for f in futures if not f.exception()]

            errors = [f.exception() for f in futures if f.exception()]

            if errors:
                return {
                    "status": config.BLOCK_ERROR,
                    "errors": [str(error) for error in errors],
                    "batch": batch,
                    "node_pair": node_pair,
                }

            hash1, hash2 = results[0][0][0], results[1][0][0]

            if hash1 != hash2:
                # Run the block query on both nodes in parallel
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [
                        executor.submit(run_query, worker_state, host1, block_sql),
                        executor.submit(run_query, worker_state, host2, block_sql),
                    ]
                    results = [f.result() for f in futures if not f.exception()]

                errors = [f.exception() for f in futures if f.exception()]

                if errors:
                    return {
                        "status": config.BLOCK_ERROR,
                        "errors": [str(error) for error in errors],
                        "batch": batch,
                        "node_pair": node_pair,
                    }

                t1_result, t2_result = results

                # Transform all elements in t1_result and t2_result into strings before
                # consolidating them into a set
                # TODO: Test and add support for different datatypes here
                t1_result = [
                    tuple(x.hex() if isinstance(x, bytes) else str(x) for x in row)
                    for row in t1_result
                ]
                t2_result = [
                    tuple(x.hex() if isinstance(x, bytes) else str(x) for x in row)
                    for row in t2_result
                ]

                # Collect results into OrderedSets for comparison
                t1_set = OrderedSet(t1_result)
                t2_set = OrderedSet(t2_result)

                t1_diff = t1_set - t2_set
                t2_diff = t2_set - t1_set

                # It is possible that the hash mismatch is a false negative.
                # E.g., if there are extraneous spaces in the JSONB column.
                # In this case, we can still consider the block to be OK.
                if (not t1_diff and not t2_diff) or (
                    len(t1_diff) == 0 and len(t2_diff) == 0
                ):
                    continue

                node_pair_key = f"{host1}/{host2}"

                if node_pair_key not in worker_diffs:
                    worker_diffs[node_pair_key] = {host1: [], host2: []}

                for row in t1_diff:
                    worker_diffs[node_pair_key][host1].append(dict(zip(cols, row)))
                for row in t2_diff:
                    worker_diffs[node_pair_key][host2].append(dict(zip(cols, row)))
                total_diffs += max(len(t1_diff), len(t2_diff))

    # Return all diffs found by this worker
    return {
        "status": config.BLOCK_OK if total_diffs == 0 else config.BLOCK_MISMATCH,
        "diffs": worker_diffs,
        "total_diffs": total_diffs,
        "batch": batch,
        "node_pair": node_pair,
    }


def table_diff(td_task: TableDiffTask, skip_all_checks: bool = False):
    """Efficiently compare tables across cluster using checksums and blocks of rows"""

    if not skip_all_checks:
        td_task = ace.table_diff_checks(td_task, skip_validation=True)

    simple_primary_key = True
    if len(td_task.fields.key.split(",")) > 1:
        simple_primary_key = False

    row_count = 0
    total_rows = 0
    conn_with_max_rows = None
    conn_list = []

    try:
        for params in td_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_name": params["dbname"],
                "db_user": params["user"],
                "db_password": params.get("password", None),
            }
            _, conn = td_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    td_task.client_role
                    if (config.USE_CERT_AUTH and td_task.invoke_method == "api")
                    else None
                ),
            )

            rows = ace.get_row_count(
                conn, td_task.fields.l_schema, td_task.fields.l_table
            )
            total_rows += rows
            if rows > row_count:
                row_count = rows
                conn_with_max_rows = conn

            conn_list.append(conn)
    except Exception as e:
        context = {"total_rows": total_rows, "mismatch": False, "errors": [str(e)]}
        ace.handle_task_exception(td_task, context)
        raise e

    pkey_offsets = []

    if not conn_with_max_rows:
        util.message(
            "ALL TABLES ARE EMPTY",
            p_state="warning",
            quiet_mode=td_task.quiet_mode,
        )
        return

    util.message(
        "Getting primary key offsets for table...",
        p_state="info",
        quiet_mode=td_task.quiet_mode,
    )

    total_blocks = row_count // td_task.block_rows
    total_blocks = total_blocks if total_blocks > 0 else 1
    cpus = cpu_count()
    max_procs = int(cpus * td_task.max_cpu_ratio) if cpus > 1 else 1

    # If we don't have enough blocks to keep all CPUs busy, use fewer processes
    procs = max_procs if total_blocks > max_procs else total_blocks

    try:
        ref_cur = conn_with_max_rows.cursor()
        ref_cur.execute(
            sql.SQL(GET_PKEY_OFFSETS).format(
                schema=sql.Identifier(td_task.fields.l_schema),
                table=sql.Identifier(td_task.fields.l_table),
                key=sql.SQL(", ").join(
                    [sql.Identifier(col) for col in td_task.fields.key.split(",")]
                ),
                num_blocks=total_blocks,
            )
        )
        pkey_offsets = ref_cur.fetchall()
    except Exception as e:
        context = {
            "total_rows": total_rows,
            "mismatch": False,
            "errors": [str(e)],
        }
        ace.handle_task_exception(td_task, context)
        raise e

    start_time = datetime.now()

    """
    Generate offsets for each process to work on.
    We go up to the max rows among all nodes because we want our set difference logic
    to capture diffs even if rows are absent in one node
    """

    cols_list = td_task.fields.cols.split(",")

    diff_dict = {}

    stop_event = Manager().Event()

    # TODO: Clean this up
    # Shared variables needed by all workers
    shared_objects = {
        "cluster_name": td_task.cluster_name,
        "database": td_task.fields.database,
        "node_list": td_task.fields.node_list,
        "schema_name": td_task.fields.l_schema,
        "table_name": td_task.fields.l_table,
        "cols_list": cols_list,
        "p_key": td_task.fields.key,
        "block_rows": td_task.block_rows,
        "simple_primary_key": simple_primary_key,
        "mode": "diff",
        "td_task": td_task,
        "stop_event": stop_event,
    }

    util.message(
        "Starting jobs to compare tables...\n",
        p_state="info",
        quiet_mode=td_task.quiet_mode,
    )

    batches = [
        pkey_offsets[i : i + td_task.batch_size]
        for i in range(0, len(pkey_offsets), td_task.batch_size)
    ]

    mismatch = False
    diffs_exceeded = False
    errors = False
    error_list = []
    total_diffs = 0

    try:
        with WorkerPool(
            n_jobs=procs,
            shared_objects=shared_objects,
            use_worker_state=True,
        ) as pool:
            for result in pool.imap_unordered(
                compare_checksums,
                make_single_arguments(batches),
                worker_init=init_conn_pool,
                worker_exit=close_conn_pool,
                progress_bar=True if not td_task.quiet_mode else False,
                progress_bar_style="rich",
                iterable_len=len(batches),
            ):
                if result["status"] == config.BLOCK_ERROR:
                    errors = True
                    error_list.append(
                        {
                            "node_pair": result["node_pair"],
                            "batch": result["batch"],
                            "errors": result["errors"],
                        }
                    )
                    stop_event.set()
                    break

                if result["status"] == config.BLOCK_MISMATCH:
                    mismatch = True
                    for node_pair, node_diffs in result["diffs"].items():
                        if node_pair not in diff_dict:
                            diff_dict[node_pair] = {}
                        for node, diffs in node_diffs.items():
                            if node not in diff_dict[node_pair]:
                                diff_dict[node_pair][node] = []
                            diff_dict[node_pair][node].extend(diffs)

                    total_diffs += result["total_diffs"]
                    if total_diffs >= config.MAX_DIFF_ROWS:
                        diffs_exceeded = True
                        stop_event.set()
                        break

            if diffs_exceeded:
                util.message(
                    "Prematurely terminated jobs since diffs have"
                    " exceeded MAX_ALLOWED_DIFFS",
                    p_state="warning",
                    quiet_mode=td_task.quiet_mode,
                )
    except Exception as e:
        context = {"total_rows": total_rows, "mismatch": mismatch, "errors": [str(e)]}
        ace.handle_task_exception(td_task, context)
        raise e

    run_time = util.round_timedelta(datetime.now() - start_time).total_seconds()
    run_time_str = f"{run_time:.2f}"

    td_task.scheduler.task_status = "COMPLETED"
    td_task.scheduler.finished_at = datetime.now()
    td_task.scheduler.time_taken = run_time
    td_task.scheduler.task_context = {
        "total_rows": total_rows,
        "mismatch": mismatch,
        "diffs_summary": td_task.diff_summary if mismatch else {},
        "errors": [],
    }

    if errors:
        context = {"total_rows": total_rows, "mismatch": mismatch, "errors": error_list}
        ace.handle_task_exception(td_task, context)

        # Even though we've updated the task in the DB, we still need to
        # raise an exception so that a) it comes up in the CLI and b) we
        # have a record of it in the logs
        raise AceException(
            "There were one or more errors while running the table-diff job. \n"
            "Please examine the connection information provided, or the nodes' \n"
            "status before running this script again. Error list: \n"
            f"{error_list}"
        )

    # Mismatch is True if there is a block mismatch or if we have
    # estimated that diffs may be greater than max allowed diffs
    if mismatch:
        if diffs_exceeded:
            util.message(
                f"TABLES DO NOT MATCH. DIFFS HAVE EXCEEDED {config.MAX_DIFF_ROWS} ROWS",
                p_state="warning",
                quiet_mode=td_task.quiet_mode,
            )

        else:
            util.message(
                "TABLES DO NOT MATCH", p_state="warning", quiet_mode=td_task.quiet_mode
            )

        """
        Read the result queue and count differences between each node pair
        in the cluster
        """

        for node_pair in diff_dict.keys():
            node1, node2 = node_pair.split("/")
            diff_count = max(
                len(diff_dict[node_pair][node1]), len(diff_dict[node_pair][node2])
            )
            td_task.diff_summary[node_pair] = diff_count
            util.message(
                f"FOUND {diff_count} DIFFS BETWEEN {node1} AND {node2}",
                p_state="warning",
                quiet_mode=td_task.quiet_mode,
            )

        try:
            if td_task.output == "json" or td_task.output == "html":
                td_task.diff_file_path = ace.write_diffs_json(
                    td_task,
                    diff_dict,
                    td_task.fields.col_types,
                    quiet_mode=td_task.quiet_mode,
                )

                if td_task.output == "html":
                    ace_html_reporter.generate_html(
                        td_task.diff_file_path, td_task.fields.key.split(",")
                    )
            elif td_task.output == "csv":
                ace.write_diffs_csv(diff_dict)
        except Exception as e:
            context = {
                "total_rows": total_rows,
                "mismatch": mismatch,
                "errors": [str(e)],
            }
            ace.handle_task_exception(td_task, context)
            raise e

    else:
        util.message(
            "TABLES MATCH OK\n", p_state="success", quiet_mode=td_task.quiet_mode
        )

    util.message(
        f"TOTAL ROWS CHECKED = {total_rows}\nRUN TIME = {run_time_str} seconds",
        p_state="info",
        quiet_mode=td_task.quiet_mode,
    )

    # We need to delete the view if it was created
    if td_task.table_filter:
        for param in td_task.fields.conn_params:
            node_info = {
                "public_ip": param["host"],
                "port": param["port"],
                "db_user": param["user"],
                "db_name": param["dbname"],
                "db_password": param.get("password", None),
            }
            _, conn = td_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    td_task.client_role
                    if config.USE_CERT_AUTH and td_task.invoke_method == "api"
                    else None
                ),
            )
            conn.execute(
                sql.SQL("DROP VIEW IF EXISTS {view_name}").format(
                    view_name=sql.Identifier(f"{td_task.scheduler.task_id}_view"),
                )
            )
            conn.commit()

    td_task.scheduler.task_status = "COMPLETED"
    td_task.scheduler.finished_at = datetime.now()
    td_task.scheduler.time_taken = run_time
    td_task.scheduler.task_context = {
        "total_rows": total_rows,
        "mismatch": mismatch,
        "diffs_summary": td_task.diff_summary if mismatch else {},
        "errors": [],
    }

    if not td_task.skip_db_update:
        ace_db.update_ace_task(td_task)


def table_repair(tr_task: TableRepairTask):
    """Apply changes from a table-diff source of truth to destination table"""

    tr_task = ace.table_repair_checks(tr_task, skip_validation=True)

    start_time = datetime.now()
    conns = {}

    # Instead of adding another dependency (bidict) to ace, we'll simply store the
    # host_ip:port of the source of truth node.
    source_of_truth_node_key = None

    try:
        for params in tr_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_user": params["user"],
                "db_name": params["dbname"],
                "db_password": params.get("password", None),
            }
            hostname_key = node_info["public_ip"] + ":" + node_info["port"]
            node_hostname = tr_task.fields.host_map[hostname_key]
            if tr_task.source_of_truth == node_hostname:
                source_of_truth_node_key = hostname_key

            # We will not drop privileges just yet, since we need to set
            # spock.repair_mode(true) and session_replication_role
            _, conn = tr_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    tr_task.client_role
                    if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                    else None
                ),
                drop_privileges=False,
            )
            conns[node_hostname] = conn
    except Exception as e:
        context = {"errors": [str(e)]}
        ace.handle_task_exception(tr_task, context)
        raise e

    if tr_task.generate_report:
        report = dict()
        now = datetime.now()
        report["operation_type"] = "table-repair"
        report["mode"] = "LIVE_RUN"
        report["time_stamp"] = (
            now.strftime("%Y-%m-%d %H:%M:%S") + f"{now.microsecond // 1000:03d}"
        )
        report["supplied_args"] = {
            "cluster_name": tr_task.cluster_name,
            "diff_file": tr_task.diff_file_path,
            "source_of_truth": tr_task.source_of_truth,
            "table_name": tr_task._table_name,
            "dbname": tr_task._dbname,
            "dry_run": tr_task.dry_run,
            "quiet": tr_task.quiet_mode,
            "upsert_only": tr_task.upsert_only,
            "generate_report": tr_task.generate_report,
        }
        report["database_credentials_used"] = tr_task.fields.database
        report["changes"] = dict()

    def generate_report():
        now = datetime.now()
        report_folder = "reports"
        report["run_time"] = util.round_timedelta(
            datetime.now() - start_time
        ).total_seconds()

        if not os.path.exists(report_folder):
            os.mkdir(report_folder)

        dirname = now.strftime("%Y-%m-%d")
        diff_file_suffix = now.strftime("%H%M%S") + f"{now.microsecond // 1000:03d}"
        if tr_task.dry_run:
            diff_filename = "dry_run_report_" + diff_file_suffix + ".json"
        else:
            diff_filename = "repair_report_" + diff_file_suffix + ".json"

        dirname = os.path.join(report_folder, dirname)

        if not os.path.exists(dirname):
            os.mkdir(dirname)

        filename = os.path.join(dirname, diff_filename)

        try:
            json.dump(report, open(filename, "w"), default=str, indent=2)
        except Exception as e:
            context = {"errors": [f"Could not write report: {str(e)}"]}
            ace.handle_task_exception(tr_task, context)
            raise e

        util.message(f"Wrote report to {filename}", quiet_mode=tr_task.quiet_mode)

    diff_json = ace.check_diff_file_format(tr_task.diff_file_path, tr_task)

    # Remove metadata columsn "_Spock_CommitTS_" and "_Spock_CommitOrigin_"
    # from cols_list
    cols_list = tr_task.fields.cols.split(",")
    cols_list = [col for col in cols_list if not col.startswith("_Spock_")]

    simple_primary_key = True
    keys_list = []
    if len(tr_task.fields.key.split(",")) > 1:
        simple_primary_key = False
        keys_list = tr_task.fields.key.split(",")
    else:
        simple_primary_key = True
        keys_list = [tr_task.fields.key]

    """
    The structure of the diff_json is as follows:
    {
        "node1/node2": {
            "node1": [row1, row2, row3],
            "node2": [row1, row2, row3]
        },
        "node1/node3": {
            "node1": [row1, row2, row3],
            "node3": [row1, row2, row3]
        }
    }

    true_rows extracts all rows from the source of truth node across all node diffs
    and dedupes them.
    We use true_rows if the node_pair does not contain the source of truth node.
    This case can happen only when another node has the same rows as the source of
    truth node, but the third node has some differences. The proof of this is trivial,
    and is left as an exercise to the reader.

    Strategy for repair:
    1.  Extract all rows from source of truth node across all node_pairs
        and store them in true_rows.
    2.  Check to see if the node_pair has the source of truth node.
        If yes, then use rows from the source of truth node in the node_pair.
        Otherwise, use true_rows obtained in step 1. At the end of this step,
        true_rows either has node-specific diffs or all diffs--as computed in step 1.
    3.  We could have four different situations when checking true_rows
        against other nodes:
        a.  Rows are present in source of truth node but not in other nodes.
        b.  Rows are present in source of truth node and in other nodes but have
            different values.
        c.  Rows are present in other nodes but not in source of truth node.
        d.  Both source of truth node and the other node have rows, but some need to be
            inserted, some deleted and some updated.

    4.  Here is how we handle each of these situations:
        a.  We simply insert the rows into the other nodes.
        b.  We update the rows in the other nodes with the values from the source of
            truth node.
        c.  We delete the rows from the other nodes.
        d.  We insert/update/delete rows in the other nodes as needed.

    5.  But how do we know which rows to insert/update/delete? We need to do some
        set operations to figure this out. We will call the source of truth node
        as the ST node and the other node as the 'D' (divergent) node. We perform
        these set operations using the primary key of the table.
        a.  Rows to insert in D node: ST node - D node
        b.  Rows to update in D node: ST node intersection D node
        c.  Rows to delete in D node: D node - ST node

    """

    full_rows_to_upsert = dict()
    full_rows_to_delete = dict()
    other_nodes = set()

    for node_pair, node_data in diff_json.items():
        node1, node2 = node_pair.split("/")

        if node1 == tr_task.source_of_truth:
            pass
        elif node2 == tr_task.source_of_truth:
            node2 = node1
            node1 = tr_task.source_of_truth
        else:
            continue

        other_nodes.add(node2)
        main_rows = {
            tuple(row[key] for key in keys_list): row for row in node_data[node1]
        }
        side_rows = {
            tuple(row[key] for key in keys_list): row for row in node_data[node2]
        }

        full_rows_to_upsert[node2] = main_rows
        full_rows_to_delete[node2] = {
            key: val for key, val in side_rows.items() if key not in main_rows
        }

    """
    Format of full_rows_to_upsert = {
        'node2': {
            pkey to upsert: full row of pkey ...
        },
        'node3: {
            pkey to upsert: full row of pkey ...
        },
    }

    Format of full_rows_to_delete = {
        'node2': {
            pkey to delete: full row of pkey ...
        },
        'node3: {
            pkey to delete: full row of pkey ...
        },
    }

    Format of other_nodes = {
        node1,
        node2,
    }
    """

    if tr_task.dry_run:
        dry_run_msg = "######## DRY RUN ########\n\n"

        if tr_task.generate_report:
            report["operation_type"] = "DRY_RUN"

        for node in other_nodes:
            if not tr_task.upsert_only and not tr_task.insert_only:
                dry_run_msg += (
                    "Repair would have attempted to upsert "
                    + f"{len(full_rows_to_upsert[node])} rows and delete "
                    + f"{len(full_rows_to_delete[node])} rows on { node }\n"
                )

                if tr_task.generate_report:
                    report["changes"][node] = {
                        "would_upsert": list(full_rows_to_upsert[node].values()),
                        "would_delete": list(full_rows_to_delete[node].values()),
                    }
            elif tr_task.insert_only:
                dry_run_msg += (
                    "Repair would have attempted to insert "
                    + f"{len(full_rows_to_upsert[node])} rows on { node }\n"
                )

                if len(full_rows_to_delete[node]) > 0:
                    dry_run_msg += (
                        "There are an additional "
                        + f"{len(full_rows_to_delete[node])} rows on { node }"
                        + f" not present on { tr_task.source_of_truth }\n"
                    )

                if tr_task.generate_report:
                    report["changes"][node] = {
                        "would_insert": list(full_rows_to_upsert[node].values()),
                        "skipped_deletes": list(full_rows_to_delete[node].values()),
                        "skipped_updates": list(full_rows_to_upsert[node].values()),
                    }
            else:
                dry_run_msg += (
                    "Repair would have attempted to upsert "
                    + f"{len(full_rows_to_upsert[node])} rows on { node }\n"
                )

                if len(full_rows_to_delete[node]) > 0:
                    dry_run_msg += (
                        "There are an additional "
                        + f"{len(full_rows_to_delete[node])} rows on { node }"
                        + f" not present on { tr_task.source_of_truth }\n"
                    )

                if tr_task.generate_report:
                    report["changes"][node] = {
                        "would_upsert": list(full_rows_to_upsert[node].values()),
                        "skipped_deletes": list(full_rows_to_delete[node].values()),
                    }

        dry_run_msg += "\n######## END DRY RUN ########"
        util.message(dry_run_msg, p_state="alert", quiet_mode=tr_task.quiet_mode)

        if tr_task.generate_report:
            generate_report()

        return

    total_upserted = {}
    total_deleted = {}

    col_types = tr_task.fields.col_types[source_of_truth_node_key]

    deletes_skipped = dict()

    for divergent_node in other_nodes:

        try:
            conn = conns[divergent_node]
            cur = conn.cursor()
            spock_version = ace.get_spock_version(conn)

            if spock_version >= config.SPOCK_REPAIR_MODE_MIN_VERSION:
                cur.execute("SELECT spock.repair_mode(true);")
                if tr_task.fire_triggers:
                    cur.execute("SET session_replication_role = 'local';")
                else:
                    cur.execute("SET session_replication_role = 'replica';")

            # The cursor we created earlier had superuser privileges.
            # We need to close and recreate it.
            cur.close()
        except Exception as e:
            context = {"errors": [f"Could not set repair mode: {str(e)}"]}
            ace.handle_task_exception(tr_task, context)
            raise e

        """
        The assumption we make here is that the credentials provided to ACE
        are superuser credentials. This is required since we need to have access
        to the spock schema as well as set the session_replication_role parameter.

        Non-privileged users intending to use ACE must be provided with client
        certificates, and should only be allowed to invoke ACE via the API.
        Anybody who has access to the instance/container running ACE, and can use
        the CLI is assumed to be a superuser.
        """
        tr_task.connection_pool.drop_privileges(
            conn,
            (
                tr_task.client_role
                if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                else None
            ),
        )

        # Now this will have lower privileges
        cur = conn.cursor()

        rows_to_upsert_json = full_rows_to_upsert[divergent_node].values()
        delete_keys = list(full_rows_to_delete[divergent_node].keys())

        """
        Here we are constructing an UPSERT query from true_rows and
        applying it to all nodes
        """

        table_name_sql = f'{tr_task.fields.l_schema}."{tr_task.fields.l_table}"'
        if simple_primary_key:
            update_sql = f"""
            INSERT INTO {table_name_sql}
            VALUES ({','.join(['%s'] * len(cols_list))})
            ON CONFLICT ("{tr_task.fields.key}") DO """
        else:
            update_sql = f"""
            INSERT INTO {table_name_sql}
            VALUES ({','.join(['%s'] * len(cols_list))})
            ON CONFLICT
            ({','.join(['"' + col + '"' for col in keys_list])}) DO """

        if tr_task.insert_only:
            update_sql += "NOTHING;"
        else:
            update_sql += "UPDATE SET "
            for col in cols_list:
                update_sql += f'"{col}" = EXCLUDED."{col}", '
            update_sql = update_sql[:-2] + ";"

        delete_sql = None

        if simple_primary_key:
            delete_sql = f"""
            DELETE FROM {table_name_sql}
            WHERE "{tr_task.fields.key}" = %s;
            """
        else:
            delete_sql = f"""
            DELETE FROM {table_name_sql}
            WHERE
            """

            where_conditions = []
            for key in keys_list:
                where_conditions.append(f'"{key}" = %s')
            delete_sql += " AND ".join(where_conditions) + ";"

        if tr_task.generate_report:
            report["changes"][divergent_node] = dict()

        upsert_tuples = ace.convert_json_to_pg_type(
            rows_to_upsert_json, cols_list, col_types
        )

        try:
            # Let's first delete rows to avoid secondary unique key violations
            if delete_keys and not tr_task.upsert_only and not tr_task.insert_only:
                # Performing the deletes
                cur.executemany(delete_sql, delete_keys)
                if tr_task.generate_report:
                    report["changes"][divergent_node]["deleted_rows"] = delete_keys
            elif delete_keys and (tr_task.upsert_only or tr_task.insert_only):
                deletes_skipped[divergent_node] = delete_keys

            # Now perform the upsert
            cur.executemany(update_sql, upsert_tuples)
            if tr_task.generate_report:
                report["changes"][divergent_node]["upserted_rows"] = [
                    dict(zip(cols_list, tup)) for tup in upsert_tuples
                ]

            if spock_version >= config.SPOCK_REPAIR_MODE_MIN_VERSION:
                # Temporarily elevate privileges to disable repair mode
                with conn.cursor() as super_cur:
                    # Reset to superuser role
                    super_cur.execute("RESET ROLE")
                    super_cur.execute("SELECT spock.repair_mode(false);")

                # This is not really necessary here since we're done repairing,
                # but we'll have it just in case.
                if tr_task.invoke_method == "api":
                    tr_task.connection_pool.drop_privileges(
                        conn,
                        (
                            tr_task.client_role
                            if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                            else None
                        ),
                    )

            conn.commit()
        except Exception as e:
            conn.rollback()
            context = {"errors": [f"Could not perform repairs: {str(e)}"]}
            ace.handle_task_exception(tr_task, context)
            raise e
        finally:
            if cur:
                cur.close()

    if tr_task.upsert_only:

        def compare_values(val1: dict, val2: dict) -> bool:
            if val1.keys() != val2.keys():
                return False
            if any([val1[key] != val2[key] for key in val1.keys()]):
                return False
            return True

        upsert_dict = dict()
        for nd_name, keys in deletes_skipped.items():
            for key in keys:
                if simple_primary_key:
                    full_key = key
                else:
                    full_key = tuple(pkey_part for pkey_part in key)

                if full_key not in upsert_dict:
                    upsert_dict[full_key] = full_rows_to_delete[nd_name][key], {nd_name}
                elif not compare_values(
                    full_rows_to_delete[nd_name][key], upsert_dict[full_key][0]
                ):
                    upsert_dict[full_key][1].add(nd_name)
                else:
                    upsert_dict[full_key][1].add(nd_name)

        # format of upsert dict will now be
        # {
        #     "pkey1" : ({key: val to upsert to pkey1}, {nodes that have pkey1})
        #     "pkey2" : ({key: val to upsert to pkey2}, {nodes that have pkey2})
        # }
        # example when n1 is s.o.t.
        # {
        #     '2': ({'id': '2', 'num': '2'}, {'n2'}),
        #     '4': ({'id': '4', 'num': '4'}, {'n3', 'n2'}),
        #     '5': ({'id': '5', 'num': '4'}, {'n3', 'n2'}),
        #     '3': ({'id': '3', 'num': '3'}, {'n3'})
        # }
        # so `2`, `3`, `4`, `5` need to be inserted into n1, `2` in n3, and `3` into n2

        if tr_task.generate_report:
            report["changes"][tr_task.source_of_truth] = dict()
            report["changes"][tr_task.source_of_truth]["upserted_rows"] = []
            report["changes"][tr_task.source_of_truth]["deleted_rows"] = []
            report["changes"][tr_task.source_of_truth]["missing_rows"] = [
                {"row": values, "present_in": list(nodes)}
                for values, nodes in upsert_dict.values()
            ]

    run_time = util.round_timedelta(datetime.now() - start_time).total_seconds()
    run_time_str = f"{run_time:.2f}"

    util.message(
        f"Successfully applied diffs to {tr_task._table_name} in cluster "
        f"{tr_task.cluster_name}\n",
        p_state="success",
        quiet_mode=tr_task.quiet_mode,
    )

    if tr_task.generate_report:
        generate_report()

    util.message("*** SUMMARY ***\n", p_state="info", quiet_mode=tr_task.quiet_mode)

    for node in other_nodes:
        if tr_task.insert_only:
            util.message(
                f"{node} INSERTED = {len(full_rows_to_upsert[node])} rows",
                p_state="info",
                quiet_mode=tr_task.quiet_mode,
            )
        else:
            util.message(
                f"{node} UPSERTED = {len(full_rows_to_upsert[node])} rows",
                p_state="info",
                quiet_mode=tr_task.quiet_mode,
            )

    print()

    if not tr_task.upsert_only and not tr_task.insert_only:
        for node in other_nodes:
            util.message(
                f"{node} DELETED = {len(full_rows_to_delete[node])} rows",
                p_state="info",
                quiet_mode=tr_task.quiet_mode,
            )

    util.message(
        f"RUN TIME = {run_time_str} seconds",
        p_state="info",
        quiet_mode=tr_task.quiet_mode,
    )

    if spock_version < 4.0:
        util.message(
            "WARNING: Unable to pause/resume replication during repair due to"
            "an older spock version. Please do a manual check as repair may"
            "have caused further divergence",
            p_state="warning",
            quiet_mode=tr_task.quiet_mode,
        )

    tr_task.scheduler.task_status = "COMPLETED"
    tr_task.scheduler.finished_at = datetime.now()
    tr_task.scheduler.time_taken = run_time
    tr_task.scheduler.task_context = {
        "source_of_truth": tr_task.source_of_truth,
        "upserted": total_upserted,
        "deleted": total_deleted,
    }
    ace_db.update_ace_task(tr_task)


"""
This is a special case of table-repair where updates have propagated to nodes,
but some columns have null values. We consult the diff file and fill in the nulls
with the values from the other node--and, we do it on both sides.

For example, if we have a table with columns `id`, `fname`, `lname`, and `age`,
and we have a diff file that shows that `fname` and `lname` are updated on node1,
but `age` is null on node1 and non-null on node2. Also suppose that lname is null
on node2 and non-null on node1.

We will fill in the null `age` values on node1 with the non-null `age` values from
node2. We will also fill in the null `lname` values on node2 with the non-null
`lname` values from node1.

This can very quickly become an expensive operation if there are many rows with
null values. So, we take a different approach here. We create a temporary table
to store the values that need to be updated. We then perform a single UPDATE with
a JOIN to efficiently update all rows at once.

"""


def table_repair_fix_nulls(tr_task: TableRepairTask) -> None:
    """Fix null values between nodes by synchronizing non-null values.

    Creates a temporary table to store values that need to be updated, then
    performs a single UPDATE with a JOIN to efficiently update all rows at once.
    """

    tr_task = ace.table_repair_checks(tr_task, skip_validation=True)

    start_time = datetime.now()
    conns = {}
    simple_primary_key = True
    keys_list = tr_task.fields.key.split(",")

    if len(keys_list) > 1:
        simple_primary_key = False

    try:
        for params in tr_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_user": params["user"],
                "db_name": params["dbname"],
                "db_password": params.get("password", None),
            }

            hostname_key = node_info["public_ip"] + ":" + node_info["port"]
            node_hostname = tr_task.fields.host_map[hostname_key]
            _, conn = tr_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    tr_task.client_role
                    if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                    else None
                ),
                drop_privileges=False,
            )
            conns[node_hostname] = conn
    except Exception as e:
        context = {"errors": [str(e)]}
        ace.handle_task_exception(tr_task, context)
        raise e

    diff_json = ace.check_diff_file_format(tr_task.diff_file_path, tr_task)

    # We are working with the assumption that column types are the same across nodes
    col_types = next(iter(tr_task.fields.col_types.values()))

    # Create temp table SQL--this will store the values that need to be updated
    if simple_primary_key:
        create_temp_sql = sql.SQL(
            """
            CREATE TEMP TABLE {null_updates} (
                pkey {pkey_type},
                column_name text,
                column_value text
            )
        """
        ).format(
            null_updates=sql.Identifier(f"{tr_task.scheduler.task_id}_null_updates"),
            pkey_type=sql.SQL(col_types[keys_list[0]]),
        )
    else:
        create_temp_sql = sql.SQL(
            """
            CREATE TEMP TABLE {null_updates} (
                {pkey_columns},
                column_name text,
                column_value text
            )
        """
        ).format(
            null_updates=sql.Identifier(f"{tr_task.scheduler.task_id}_null_updates"),
            pkey_columns=sql.SQL(",\n").join(
                sql.SQL("{} {}").format(sql.Identifier(key), sql.SQL(col_types[key]))
                for key in keys_list
            ),
        )

    node_pairs = list(diff_json.items())
    total_pairs = len(node_pairs)

    if not tr_task.quiet_mode:
        util.message("\nRepairing null values:", p_state="info")
        progress = tqdm(
            total=100,
            desc="Overall progress",
            unit="%",
        )
        progress_pct = 0
        progress_step = 100.0 / (total_pairs * 2)

    for pair_idx, (node_pair, pair_diffs) in enumerate(node_pairs):
        node1, node2 = node_pair.split("/")
        node1_rows = pair_diffs[node1]
        node2_rows = pair_diffs[node2]

        if not tr_task.quiet_mode:
            util.message(f"\nProcessing node pair {node_pair}", p_state="info")

        # Create lookup dictionaries for faster access
        def get_key_tuple(row):
            return tuple(row[key] for key in keys_list)

        node1_dict = {get_key_tuple(row): row for row in node1_rows}
        node2_dict = {get_key_tuple(row): row for row in node2_rows}

        # Find common keys between nodes
        common_keys = set(node1_dict.keys()) & set(node2_dict.keys())

        for node, conn in [(node1, conns[node1]), (node2, conns[node2])]:
            spock_version = None
            try:
                spock_version = ace.get_spock_version(conn)
                super_cur = conn.cursor()

                if spock_version >= config.SPOCK_REPAIR_MODE_MIN_VERSION:
                    super_cur.execute("SELECT spock.repair_mode(true);")
                    if tr_task.fire_triggers:
                        super_cur.execute("SET session_replication_role = 'local';")
                    else:
                        super_cur.execute("SET session_replication_role = 'replica';")

                super_cur.close()
            except Exception as e:
                context = {"errors": [f"Error setting repair mode on {node}: {str(e)}"]}
                ace.handle_task_exception(tr_task, context)
                raise e

            if not tr_task.quiet_mode:
                util.message(f"Processing node {node}", p_state="info")

            try:
                tr_task.connection_pool.drop_privileges(
                    conn,
                    client_role=(
                        tr_task.client_role
                        if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                        else None
                    ),
                )
                with conn.cursor() as cur:
                    cur.execute(create_temp_sql)

                    # Prepare batch insert into temp table
                    insert_values = []
                    for key in common_keys:
                        row1 = node1_dict[key]
                        row2 = node2_dict[key]

                        # Compare all columns
                        for col in set(row1.keys()) | set(row2.keys()):
                            # TODO: What if the primary key itself, or one of
                            # the keys in a composite pkey, is null?
                            if col in keys_list:  # Skip primary key columns
                                continue

                            val1 = row1.get(col)
                            val2 = row2.get(col)

                            # If this node has null and other has non-null
                            if (
                                node == node1
                                and (val1 is None or val1 == "")
                                and val2 is not None
                            ):
                                # Handle JSON values
                                if isinstance(val2, (dict, list)):
                                    val2 = json.dumps(val2)

                                if simple_primary_key:
                                    insert_values.append((key[0], col, val2))
                                else:
                                    insert_values.append(key + (col, val2))
                            elif (
                                node == node2
                                and (val2 is None or val2 == "")
                                and val1 is not None
                            ):
                                # Handle JSON values
                                if isinstance(val1, (dict, list)):
                                    val1 = json.dumps(val1)

                                if simple_primary_key:
                                    insert_values.append((key[0], col, val1))
                                else:
                                    insert_values.append(key + (col, val1))

                    # Batch insert into temp table
                    if simple_primary_key:
                        cur.executemany(
                            sql.SQL(
                                "INSERT INTO {null_updates} VALUES (%s, %s, %s)"
                            ).format(
                                null_updates=sql.Identifier(
                                    f"{tr_task.scheduler.task_id}_null_updates"
                                )
                            ),
                            insert_values,
                        )
                    else:
                        placeholders = sql.SQL(", ").join(
                            [sql.SQL("%s")] * (len(keys_list) + 2)
                        )
                        query = sql.SQL(
                            "INSERT INTO {null_updates} VALUES ({placeholders})"
                        ).format(
                            null_updates=sql.Identifier(
                                f"{tr_task.scheduler.task_id}_null_updates"
                            ),
                            placeholders=placeholders,
                        )
                        cur.executemany(query, insert_values)

                    # Get unique columns that need updates
                    cur.execute(
                        sql.SQL(
                            "SELECT DISTINCT column_name FROM {null_updates}"
                        ).format(
                            null_updates=sql.Identifier(
                                f"{tr_task.scheduler.task_id}_null_updates"
                            )
                        )
                    )
                    columns_to_update = [row[0] for row in cur.fetchall()]

                    if not tr_task.quiet_mode:
                        if columns_to_update:
                            util.message(
                                f"Updating {len(columns_to_update)} columns on {node}",
                                p_state="info",
                            )
                        else:
                            util.message(
                                f"No updates needed for {node}", p_state="info"
                            )

                    # Perform the UPDATE with dynamic column setting
                    update_sql = sql.SQL(
                        """
                        UPDATE {table_name} t
                        SET {column} = u.column_value::{type}
                        FROM {null_updates} u
                        WHERE {join_condition}
                        AND u.column_name = {column_name}
                    """
                    )

                    for col in columns_to_update:
                        if simple_primary_key:
                            join_cond = sql.SQL("t.{} = u.pkey").format(
                                sql.Identifier(keys_list[0])
                            )
                        else:
                            join_cond = sql.SQL(" AND ").join(
                                sql.SQL("t.{} = u.{}").format(
                                    sql.Identifier(key), sql.Identifier(key)
                                )
                                for key in keys_list
                            )

                        update_sql_formatted = update_sql.format(
                            null_updates=sql.Identifier(
                                f"{tr_task.scheduler.task_id}_null_updates"
                            ),
                            table_name=sql.SQL("{}.{}").format(
                                sql.Identifier(tr_task.fields.l_schema),
                                sql.Identifier(tr_task.fields.l_table),
                            ),
                            column=sql.Identifier(col),
                            # Remove [] suffix while casting
                            type=sql.SQL(col_types[col].replace("[]", "")),
                            join_condition=join_cond,
                            column_name=sql.Literal(col),
                        )

                        # For array types, we need to convert the string representation
                        # from Python list format "[1,2,3]" to Postgres array
                        # format "{1,2,3}"
                        if col_types[col].endswith("[]"):
                            update_sql_formatted = sql.SQL(
                                """
                                UPDATE {table_name} t
                                SET {column} = string_to_array(
                                    trim(both '[]' from u.column_value),
                                    ','
                                )::{type}[]
                                FROM {null_updates} u
                                WHERE {join_condition}
                                AND u.column_name = {column_name}
                            """
                            ).format(
                                null_updates=sql.Identifier(
                                    f"{tr_task.scheduler.task_id}_null_updates"
                                ),
                                table_name=sql.SQL("{}.{}").format(
                                    sql.Identifier(tr_task.fields.l_schema),
                                    sql.Identifier(tr_task.fields.l_table),
                                ),
                                column=sql.Identifier(col),
                                type=sql.SQL(col_types[col].replace("[]", "")),
                                join_condition=join_cond,
                                column_name=sql.Literal(col),
                            )

                        cur.execute(update_sql_formatted)

                    # Cleanup
                    cur.execute(
                        sql.SQL("DROP TABLE {null_updates}").format(
                            null_updates=sql.Identifier(
                                f"{tr_task.scheduler.task_id}_null_updates"
                            )
                        )
                    )
                    conn.commit()

                    if not tr_task.quiet_mode:
                        # Update progress after processing each node
                        progress_pct += progress_step
                        progress.n = int(progress_pct)
                        progress.refresh()

                if spock_version >= config.SPOCK_REPAIR_MODE_MIN_VERSION:
                    with conn.cursor() as super_cur:
                        super_cur.execute("RESET ROLE")
                        super_cur.execute("SELECT spock.repair_mode(false);")

                tr_task.connection_pool.drop_privileges(
                    conn,
                    client_role=(
                        tr_task.client_role
                        if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                        else None
                    ),
                )

            except Exception as e:
                conn.rollback()
                context = {"errors": [f"Error updating nulls on {node}: {str(e)}"]}
                ace.handle_task_exception(tr_task, context)
                raise e

    if not tr_task.quiet_mode:
        progress.n = 100
        progress.refresh()
        progress.close()

    end_time = datetime.now()
    run_time = util.round_timedelta(end_time - start_time).total_seconds()
    run_time_str = f"{run_time:.2f}"

    util.message(
        f"\nSuccessfully corrected nulls in {tr_task._table_name} in cluster"
        f" {tr_task.cluster_name}\n",
        p_state="success",
        quiet_mode=tr_task.quiet_mode,
    )

    util.message(
        f"RUN TIME = {run_time_str} seconds",
        p_state="info",
        quiet_mode=tr_task.quiet_mode,
    )


def table_repair_bidirectional(tr_task: TableRepairTask) -> None:
    """
    Perform bidirectional repair between nodes by inserting missing rows.

    This function performs bidirectional repair between nodes in a cluster.
    For each node pair, it will insert rows that exist in one node but not in the other.

    For example, if n1 has IDs 1-15, and n2 has IDs 1-10 and 16-20:
    After repair, both n1 and n2 will end up with IDs 1-20

    Note: This option uses `INSERT INTO ... ON CONFLICT DO NOTHING`. So, if there are
    identical rows with different values, this option alone is not enough to fully
    repair the table.

    Args:
        tr_task: The table repair task configuration
    """
    tr_task = ace.table_repair_checks(tr_task, skip_validation=True)

    start_time = datetime.now()
    conns = {}
    simple_primary_key = True
    keys_list = tr_task.fields.key.split(",")
    report = {"operation_type": "REPAIR", "changes": {}}

    if len(keys_list) > 1:
        simple_primary_key = False

    try:
        for params in tr_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_user": params["user"],
                "db_name": params["dbname"],
                "db_password": params.get("password", None),
            }

            hostname_key = node_info["public_ip"] + ":" + node_info["port"]
            node_hostname = tr_task.fields.host_map[hostname_key]
            _, conn = tr_task.connection_pool.get_cluster_node_connection(
                node_info,
                tr_task.cluster_name,
                invoke_method=tr_task.invoke_method,
                client_role=(
                    tr_task.client_role
                    if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                    else None
                ),
                drop_privileges=False,
            )
            conns[node_hostname] = conn
    except Exception as e:
        context = {"errors": [str(e)]}
        ace.handle_task_exception(tr_task, context)
        raise e

    diff_json = ace.check_diff_file_format(tr_task.diff_file_path, tr_task)

    cols_list = tr_task.fields.cols.split(",")
    cols_list = [col for col in cols_list if not col.startswith("_Spock_")]

    col_types = next(iter(tr_task.fields.col_types.values()))

    node_pairs = list(diff_json.items())
    total_pairs = len(node_pairs)

    util.message(
        "\nPerforming bidirectional repair:",
        p_state="info",
        quiet_mode=tr_task.quiet_mode,
    )

    progress = tqdm(
        total=100,
        desc="Overall progress",
        unit="%",
    )

    table_name_sql = f'{tr_task.fields.l_schema}."{tr_task.fields.l_table}"'
    if simple_primary_key:
        insert_sql = f"""
        INSERT INTO {table_name_sql}
        VALUES ({','.join(['%s'] * len(cols_list))})
        ON CONFLICT ("{tr_task.fields.key}") DO NOTHING;"""
    else:
        insert_sql = f"""
        INSERT INTO {table_name_sql}
        VALUES ({','.join(['%s'] * len(cols_list))})
        ON CONFLICT
        ({','.join(['"' + col + '"' for col in keys_list])}) DO NOTHING;"""

    if tr_task.dry_run:
        dry_run_msg = "######## DRY RUN ########\n\n"
        if tr_task.generate_report:
            report["operation_type"] = "DRY_RUN"

    progress_pct = 0
    progress_step = 100.0 / (total_pairs * 2)

    for _, (node_pair, pair_diffs) in enumerate(node_pairs):
        node1, node2 = node_pair.split("/")
        node1_rows = pair_diffs[node1]
        node2_rows = pair_diffs[node2]

        util.message(
            f"\nProcessing node pair {node_pair}",
            p_state="info",
            quiet_mode=tr_task.quiet_mode,
        )

        def get_key_tuple(row):
            return tuple(row[key] for key in keys_list)

        node1_dict = {get_key_tuple(row): row for row in node1_rows}
        node2_dict = {get_key_tuple(row): row for row in node2_rows}

        # Find rows unique to each node
        node1_only_keys = set(node1_dict.keys()) - set(node2_dict.keys())
        node2_only_keys = set(node2_dict.keys()) - set(node1_dict.keys())

        n1_inserts = [node2_dict[key] for key in node2_only_keys]
        n2_inserts = [node1_dict[key] for key in node1_only_keys]

        if tr_task.dry_run:
            dry_run_msg += f"Node pair: {node_pair}\n"
            dry_run_msg += f"Rows unique to {node1}: {len(node1_only_keys)}\n"
            dry_run_msg += f"Rows unique to {node2}: {len(node2_only_keys)}\n\n"

            if tr_task.generate_report:
                report["changes"][node_pair] = {
                    f"{node1}_only": n1_inserts,
                    f"{node2}_only": n2_inserts,
                }
            continue

        def perform_inserts(target_node, conn, insert_rows):
            util.message(
                f"Processing node {target_node}",
                p_state="info",
                quiet_mode=tr_task.quiet_mode,
            )

            try:
                super_cur = conn.cursor()
                spock_version = ace.get_spock_version(conn)

                if spock_version >= config.SPOCK_REPAIR_MODE_MIN_VERSION:
                    super_cur.execute("SELECT spock.repair_mode(true);")
                    if tr_task.fire_triggers:
                        super_cur.execute("SET session_replication_role = 'local';")
                    else:
                        super_cur.execute("SET session_replication_role = 'replica';")

                super_cur.close()

                tr_task.connection_pool.drop_privileges(
                    conn,
                    client_role=(
                        tr_task.client_role
                        if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                        else None
                    ),
                )
            except Exception as e:
                context = {
                    "errors": [
                        f"Could not set repair mode and"
                        f" drop privileges on {target_node}: {str(e)}"
                    ]
                }
                ace.handle_task_exception(tr_task, context)
                raise e

            try:
                # We need this lower-privileged cursor to perform the inserts
                cur = conn.cursor()
                insert_tuples = ace.convert_json_to_pg_type(
                    insert_rows, cols_list, col_types
                )

                if insert_tuples:
                    cur.executemany(insert_sql, insert_tuples)
                    if tr_task.generate_report:
                        report["changes"][target_node] = {
                            "inserted_rows": [
                                dict(zip(cols_list, tup)) for tup in insert_tuples
                            ]
                        }

                if spock_version >= config.SPOCK_REPAIR_MODE_MIN_VERSION:
                    # Temporarily elevate privileges to disable repair mode
                    with conn.cursor() as super_cur:
                        # Reset to superuser role
                        super_cur.execute("RESET ROLE")
                        super_cur.execute("SELECT spock.repair_mode(false);")

                tr_task.connection_pool.drop_privileges(
                    conn,
                    (
                        tr_task.client_role
                        if config.USE_CERT_AUTH and tr_task.invoke_method == "api"
                        else None
                    ),
                )

                conn.commit()
                cur.close()

            except Exception as e:
                conn.rollback()
                error_msg = "Error during bidirectional repair on"
                f" {target_node}: {str(e)}"
                context = {"errors": [error_msg]}
                ace.handle_task_exception(tr_task, context)
                raise e

        perform_inserts(target_node=node1, conn=conns[node1], insert_rows=n1_inserts)
        perform_inserts(target_node=node2, conn=conns[node2], insert_rows=n2_inserts)

        if not tr_task.quiet_mode:
            progress_pct += progress_step
            progress.n = int(progress_pct)
            progress.refresh()

    if tr_task.dry_run:
        dry_run_msg += "\n######## END DRY RUN ########"
        util.message(dry_run_msg, p_state="alert", quiet_mode=tr_task.quiet_mode)

        if tr_task.generate_report:
            ace.generate_report(tr_task, report)

        return

    if not tr_task.quiet_mode:
        progress.n = 100
        progress.refresh()
        progress.close()

    end_time = datetime.now()
    run_time = util.round_timedelta(end_time - start_time).total_seconds()
    run_time_str = f"{run_time:.2f}"

    msg = (
        f"\nSuccessfully completed bidirectional repair in {tr_task._table_name} "
        f"in cluster {tr_task.cluster_name}\n"
    )
    util.message(msg, p_state="success", quiet_mode=tr_task.quiet_mode)

    util.message(
        f"RUN TIME = {run_time_str} seconds",
        p_state="info",
        quiet_mode=tr_task.quiet_mode,
    )


def table_rerun_temptable(td_task: TableDiffTask) -> None:

    td_task = ace.table_diff_checks(td_task, skip_validation=True)

    # load diff data and validate
    diff_data = json.load(open(td_task.diff_file_path, "r"))
    diff_keys = set()
    key = td_task.fields.key.split(",")

    # Strip any existing quotes from key names
    key = [k.strip('"') for k in key]

    # Simple pkey
    if len(key) == 1:
        for node_pair in diff_data["diffs"].keys():
            nd1, nd2 = node_pair.split("/")

            for row in (
                diff_data["diffs"][node_pair][nd1] + diff_data["diffs"][node_pair][nd2]
            ):
                diff_keys.add(row[key[0]])

    # Comp pkey
    else:
        for node_pair in diff_data["diffs"].keys():
            nd1, nd2 = node_pair.split("/")

            for row in (
                diff_data["diffs"][node_pair][nd1] + diff_data["diffs"][node_pair][nd2]
            ):
                diff_keys.add(tuple(row[key_component] for key_component in key))

    temp_table_name = f"temp_{td_task.scheduler.task_id.lower()}_rerun"
    schema = td_task._table_name.split(".")[0]
    table = td_task._table_name.split(".")[1].strip('"')

    # Use proper quoting for schema and table names
    table_qry = (
        f'CREATE TABLE {temp_table_name} AS SELECT * FROM "{schema}"."{table}" WHERE '
    )
    table_qry += generate_where_clause(key, diff_keys)
    clean_qry = f"DROP TABLE {temp_table_name}"

    if len(key) == 1:
        # Quote the column name for single primary key
        pkey_qry = f'ALTER TABLE {temp_table_name} ADD PRIMARY KEY ("{key[0]}")'
    else:
        # Quote each column name for composite primary key
        pkey_columns = ", ".join(f'"{k}"' for k in key)
        pkey_qry = f"ALTER TABLE {temp_table_name} ADD PRIMARY KEY ({pkey_columns})"

    conn_list = []
    required_privileges = ["CREATE"]

    try:
        for params in td_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_user": params["user"],
                "db_name": params["dbname"],
                "db_password": params.get("password", None),
            }
            _, conn = td_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    td_task.client_role
                    if config.USE_CERT_AUTH and td_task.invoke_method == "api"
                    else None
                ),
            )
            conn_list.append(conn)

        for con in conn_list:
            authorised, missing_privileges = ace.check_user_privileges(
                con, con.info.user, schema, table, required_privileges
            )
            # Missing privileges come back as table_<privilege>, but we use
            # "CREATE/SELECT/INSERT/UPDATE/DELETE" in the required_privileges list
            # So, we're simply formatting it correctly here for the exception
            # message
            missing_privs = [
                m.split("_")[1].upper()
                for m in missing_privileges
                if m.split("_")[1].upper() in required_privileges
            ]
            exception_msg = (
                f'User "{con.info.user}" does not have the necessary privileges'
                f" to run {', '.join(missing_privs)} "
                f'on table "{schema}.{table}" on node "{con.info.host}"'
            )

            if not authorised:
                raise AceException(exception_msg)

            cur = con.cursor()
            cur.execute(table_qry)
            cur.execute(pkey_qry)
            cur.close()
            con.commit()
    except Exception as e:
        context = {"errors": [f"Could not create temp table: {str(e)}"]}
        ace.handle_task_exception(td_task, context)
        raise e

    try:
        diff_task = td_task
        diff_task._table_name = f"public.{temp_table_name}"
        diff_task.fields.l_table = temp_table_name

        table_diff(diff_task, skip_all_checks=True)
    except Exception as e:
        context = {"errors": [f"Could not run table diff: {str(e)}"]}
        ace.handle_task_exception(td_task, context)
        raise e

    try:
        for params in td_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_user": params["user"],
                "db_name": params["dbname"],
                "db_password": params.get("password", None),
            }

            _, conn = td_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    td_task.client_role
                    if config.USE_CERT_AUTH and td_task.invoke_method == "api"
                    else None
                ),
            )
            cur = conn.cursor()
            cur.execute(clean_qry)
    except Exception as e:
        context = {"errors": [f"Could not clean up temp table: {str(e)}"]}
        ace.handle_task_exception(td_task, context)
        raise e

    td_task.scheduler.task_status = "COMPLETED"
    td_task.scheduler.finished_at = datetime.now()
    td_task.scheduler.time_taken = diff_task.scheduler.time_taken
    td_task.scheduler.task_context = diff_task.scheduler.task_context
    ace_db.update_ace_task(td_task)


def table_rerun_async(td_task: TableDiffTask) -> None:

    td_task = ace.table_diff_checks(td_task, skip_validation=True)

    # load diff data and validate
    try:
        diff_data = json.load(open(td_task.diff_file_path, "r"))
    except Exception as e:
        context = {"errors": [f"Could not read diff file: {str(e)}"]}
        ace.handle_task_exception(td_task, context)
        raise e

    diff_kset = set()
    diff_keys = list()
    key = td_task.fields.key.split(",")
    simple_primary_key = len(key) == 1

    # Simple pkey
    if simple_primary_key == 1:
        for node_pair in diff_data["diffs"].keys():
            nd1, nd2 = node_pair.split("/")

            for row in (
                diff_data["diffs"][node_pair][nd1] + diff_data["diffs"][node_pair][nd2]
            ):
                if row[key[0]] not in diff_kset:
                    diff_kset.add(row[key[0]])
                    diff_keys.append(row[key[0]])
    # Comp pkey
    else:
        for node_pair in diff_data["diffs"].keys():
            nd1, nd2 = node_pair.split("/")

            for row in (
                diff_data["diffs"][node_pair][nd1] + diff_data["diffs"][node_pair][nd2]
            ):
                element = tuple(row[key_component] for key_component in key)
                if element not in diff_kset:
                    diff_kset.add(element)
                    diff_keys.append(element)

    # create blocks
    total_rows = len(diff_kset) * len(td_task.fields.node_list)
    total_diff = len(diff_keys)

    if total_diff > 100:
        block_size = min(500, total_diff // 10)
        block_nums = ceil(total_diff / block_size)

        blocks = [
            [diff_keys[i * block_size : i * block_size + block_size]]
            for i in range(block_nums)
        ]
    else:
        blocks = [[diff_keys]]

    total_blocks = len(blocks)

    # If we don't have enough blocks to keep all CPUs busy, use fewer processes
    cpus = cpu_count()
    max_procs = int(cpus * td_task.max_cpu_ratio) if cpus > 1 else 1
    procs = max_procs if total_blocks > max_procs else total_blocks

    start_time = datetime.now()

    """
    Generate offsets for each process to work on.
    We go up to the max rows among all nodes because we want our set difference logic
    to capture diffs even if rows are absent in one node
    """

    cols_list = td_task.fields.cols.split(",")
    cols_list = [col for col in cols_list if not col.startswith("_Spock_")]

    diff_dict = {}
    stop_event = Manager().Event()

    # Shared variables needed by all workers
    shared_objects = {
        "cluster_name": td_task.cluster_name,
        "database": td_task.fields.database,
        "node_list": td_task.fields.node_list,
        "schema_name": td_task.fields.l_schema,
        "table_name": td_task.fields.l_table,
        "cols_list": cols_list,
        "p_key": td_task.fields.key,
        "block_rows": td_task.block_rows,
        "simple_primary_key": simple_primary_key,
        "mode": "rerun",
        "stop_event": stop_event,
        "td_task": td_task,
    }

    util.message(
        "Starting jobs to compare tables ...\n",
        p_state="info",
        quiet_mode=td_task.quiet_mode,
    )

    mismatch = False
    diffs_exceeded = False
    errors = False
    errors_list = []
    total_diffs = 0

    try:
        with WorkerPool(
            n_jobs=procs,
            shared_objects=shared_objects,
            use_worker_state=True,
            use_dill=True,
        ) as pool:
            for result in pool.imap_unordered(
                compare_checksums,
                make_single_arguments(blocks),
                worker_init=init_conn_pool,
                worker_exit=close_conn_pool,
                progress_bar=True if not td_task.quiet_mode else False,
                iterable_len=len(blocks),
                progress_bar_style="rich",
            ):
                if result["status"] == config.BLOCK_ERROR:
                    errors = True
                    errors_list.append(
                        {
                            "node_pair": result["node_pair"],
                            "batch": result["batch"],
                            "errors": result["errors"],
                        }
                    )
                    pool.terminate()  # Stop all workers on error
                    break

                if result["status"] == config.BLOCK_MISMATCH:
                    mismatch = True
                    # Update the diff dictionary with this worker's results
                    for node_pair, node_diffs in result["diffs"].items():
                        if node_pair not in diff_dict:
                            diff_dict[node_pair] = {}
                        for node, diffs in node_diffs.items():
                            if node not in diff_dict[node_pair]:
                                diff_dict[node_pair][node] = []
                            diff_dict[node_pair][node].extend(diffs)

                    # Update total diffs and check if exceeded
                    total_diffs += result["total_diffs"]
                    if total_diffs >= config.MAX_DIFF_ROWS:
                        diffs_exceeded = True
                        stop_event.set()  # Signal workers to stop
                        break

            if diffs_exceeded:
                util.message(
                    "Prematurely terminated jobs since diffs have"
                    " exceeded MAX_ALLOWED_DIFFS",
                    p_state="warning",
                    quiet_mode=td_task.quiet_mode,
                )
    except Exception as e:
        context = {"errors": [f"Could not spawn multiprocessing workers: {str(e)}"]}
        ace.handle_task_exception(td_task, context)
        raise e

    if errors:
        context = {
            "total_rows": total_rows,
            "mismatch": mismatch,
            "errors": errors_list,
        }
        ace.handle_task_exception(td_task, context)
        raise AceException(
            "There were one or more errors while connecting to databases."
            "Please examine the connection information provided, or the nodes"
            "status before running this script again."
            f"Errors: {errors_list}"
        )

    run_time = util.round_timedelta(datetime.now() - start_time).total_seconds()
    run_time_str = f"{run_time:.2f}"

    td_task.scheduler.task_status = "COMPLETED"
    td_task.scheduler.finished_at = datetime.now()
    td_task.scheduler.time_taken = run_time
    td_task.scheduler.task_context = {
        "total_rows": total_rows,
        "mismatch": mismatch,
    }

    # Mismatch is True if there is a block mismatch or if we have
    # estimated that diffs may be greater than max allowed diffs
    if mismatch:
        if diffs_exceeded:
            util.message(
                f"TABLES DO NOT MATCH. DIFFS HAVE EXCEEDED {config.MAX_DIFF_ROWS} ROWS",
                p_state="warning",
                quiet_mode=td_task.quiet_mode,
            )

        else:
            util.message(
                "TABLES DO NOT MATCH", p_state="warning", quiet_mode=td_task.quiet_mode
            )

        """
        Read the result queue and count differences between each node pair
        in the cluster
        """

        for node_pair in diff_dict.keys():
            node1, node2 = node_pair.split("/")
            diff_count = max(
                len(diff_dict[node_pair][node1]),
                len(diff_dict[node_pair][node2]),
            )
            td_task.diff_summary[node_pair] = diff_count
            util.message(
                f"FOUND {diff_count} DIFFS BETWEEN {node1} AND {node2}",
                p_state="warning",
                quiet_mode=td_task.quiet_mode,
            )

        td_task.scheduler.task_context["diff_summary"] = td_task.diff_summary

        try:
            if td_task.output == "json":
                td_task.diff_file_path = ace.write_diffs_json(
                    td_task,
                    diff_dict,
                    td_task.fields.col_types,
                    quiet_mode=td_task.quiet_mode,
                )

            elif td_task.output == "csv":
                ace.write_diffs_csv(diff_dict)
        except Exception as e:
            context = {"errors": [f"Could not write diffs to file: {str(e)}"]}
            ace.handle_task_exception(td_task, context)
            raise e

    else:
        util.message(
            "TABLES MATCH OK\n", p_state="success", quiet_mode=td_task.quiet_mode
        )

    util.message(
        f"TOTAL ROWS CHECKED = {total_rows}\nRUN TIME = {run_time_str} seconds",
        p_state="info",
        quiet_mode=td_task.quiet_mode,
    )
    ace_db.update_ace_task(td_task)


def multi_table_diff(task: Union[RepsetDiffTask, SchemaDiffTask]) -> None:
    """
    Reusable module for both repset-diff and schema-diff
    """

    if isinstance(task, RepsetDiffTask):
        task = ace.repset_diff_checks(task, skip_validation=True)
    elif isinstance(task, SchemaDiffTask):
        task = ace.schema_diff_checks(task, skip_validation=True)

    rd_task_context = []
    rd_start_time = datetime.now()
    errors_encountered = False

    for table in task.table_list:

        if table in task.skip_tables:
            util.message(
                f"\nSKIPPING TABLE {table}",
                p_state="info",
                quiet_mode=task.quiet_mode,
            )

            continue

        try:
            start_time = datetime.now()
            util.message(
                f"\n\nCHECKING TABLE {table}...\n",
                p_state="info",
                quiet_mode=task.quiet_mode,
            )

            td_task = TableDiffTask(
                cluster_name=task.cluster_name,
                _table_name=table,
                _dbname=task._dbname,
                fields=task.fields,
                quiet_mode=task.quiet_mode,
                block_rows=getattr(task, "block_rows", config.DIFF_BLOCK_SIZE),
                max_cpu_ratio=getattr(
                    task, "max_cpu_ratio", config.MAX_CPU_RATIO
                ),
                output=getattr(task, "output", "json"),
                _nodes=getattr(task, "_nodes", "all"),
                batch_size=getattr(task, "batch_size", config.DIFF_BATCH_SIZE),
                skip_db_update=True,
                table_filter=None,
            )

            ace.validate_table_diff_inputs(td_task)
            table_diff(td_task)
            run_time = util.round_timedelta(datetime.now() - start_time).total_seconds()
            status = {
                "table": table,
                "status": "COMPLETED",
                "time_taken": run_time,
                "total_rows": td_task.scheduler.task_context["total_rows"],
                "mismatch": td_task.scheduler.task_context["mismatch"],
                "diff_file_path": getattr(td_task, "diff_file_path", None),
            }
            td_task.connection_pool.close_all()
        except Exception as e:
            errors_encountered = True
            status = {
                "table": table,
                "status": "FAILED",
                "error": str(e),
            }
            util.message(
                f"{task.scheduler.task_type} failed for table {table} with: {str(e)}",
                p_state="warning",
            )

        rd_task_context.append(status)

    task.scheduler.task_status = "COMPLETED" if not errors_encountered else "FAILED"
    task.scheduler.finished_at = datetime.now()
    task.scheduler.task_context = rd_task_context
    task.scheduler.time_taken = util.round_timedelta(
        datetime.now() - rd_start_time
    ).total_seconds()

    ace_db.update_ace_task(task)


def spock_diff(sd_task: SpockDiffTask) -> None:
    """Compare spock meta data setup on different cluster nodes"""

    sd_task = ace.spock_diff_checks(sd_task, skip_validation=True)

    conns = {}
    compare_spock = []
    task_context = {}
    sd_start_time = datetime.now()

    print("\n")

    try:
        for params in sd_task.fields.conn_params:
            node_info = {
                "public_ip": params["host"],
                "port": params["port"],
                "db_user": params["user"],
                "db_name": params["dbname"],
                "db_password": params.get("password", None),
            }
            _, conn = sd_task.connection_pool.get_cluster_node_connection(
                node_info,
                client_role=(
                    sd_task.client_role
                    if config.USE_CERT_AUTH and sd_task.invoke_method == "api"
                    else None
                ),
            )
            conns[sd_task.fields.host_map[params["host"] + ":" + params["port"]]] = conn
    except Exception as e:
        context = {"errors": [f"Could not connect to nodes: {str(e)}"]}
        ace.handle_task_exception(sd_task, context)
        raise e

    try:
        for cluster_node in sd_task.fields.cluster_nodes:
            cur = conns[cluster_node["name"]].cursor()

            if (
                sd_task.fields.node_list
                and cluster_node["name"] not in sd_task.fields.node_list
            ):
                continue

            diff_spock = {}
            hints = []
            print(" Spock - Config " + cluster_node["name"])
            print("~~~~~~~~~~~~~~~~~~~~~~~~~")
            ace.prCyan("Node:")

            sql = """
            SELECT
                n.node_id,
                n.node_name,
                n.location,
                n.country,
                s.sub_id,
                s.sub_name,
                s.sub_enabled,
                s.sub_replication_sets
            FROM spock.node n
            LEFT OUTER JOIN spock.subscription s
            ON s.sub_target = n.node_id
            WHERE s.sub_name IS NOT NULL;
            """

            cur.execute(sql)
            node_info = cur.fetchall()

            if node_info:
                print("  " + node_info[0]["node_name"])
                diff_spock["node"] = node_info[0]["node_name"]

                ace.prCyan("  Subscriptions:")

                diff_spock["subscriptions"] = []
                for node in node_info:
                    diff_sub = {}
                    if node["sub_name"] is None:
                        hints.append(
                            "Hint: No subscriptions have been created on this node"
                        )
                    else:
                        print("    " + node["sub_name"])
                        diff_sub["sub_name"] = node["sub_name"]
                        diff_sub["sub_enabled"] = node["sub_enabled"]
                        ace.prCyan("    RepSets:")
                        diff_sub["replication_sets"] = node["sub_replication_sets"]
                        print("      " + json.dumps(node["sub_replication_sets"]))
                        if node["sub_replication_sets"] == []:
                            hints.append(
                                "Hint: No replication sets added to" " subscription"
                            )
                        diff_spock["subscriptions"].append(diff_sub)

            else:
                print(f"  No node replication in {cluster_node['name']}")

            """
            Query gets each table by which rep set they are in, values in each rep
            set are alphabetized
            """
            repset_sql = """
            SELECT
            set_name,
            array_agg(nspname || '.' || relname ORDER BY nspname, relname) as relname
            FROM (
                SELECT
                    set_name,
                    nspname,
                    relname
                FROM spock.tables
                ORDER BY set_name, nspname, relname
            ) subquery
            GROUP BY set_name
            ORDER BY set_name;
            """

            cur.execute(repset_sql)
            table_info = cur.fetchall()
            diff_spock["rep_set_info"] = []
            ace.prCyan("Tables in RepSets:")
            if table_info == []:
                hints.append("Hint: No tables in database")
            for table in table_info:
                if table["set_name"] is None:
                    print(" - Not in a replication set")
                    hints.append(
                        "Hint: Tables not in replication set might not have"
                        " primary keys, or you need to run repset-add-table"
                    )
                else:
                    print(" - " + table["set_name"])

                diff_spock["rep_set_info"].append({table["set_name"]: table["relname"]})
                print("   - ", table["relname"])

            diff_spock["hints"] = hints
            compare_spock.append(diff_spock)

            for hint in hints:
                ace.prRed(hint)
            print("\n")

    except Exception as e:
        context = {"errors": [f"Error while comparing Spock meta data: {str(e)}"]}
        ace.handle_task_exception(sd_task, context)
        raise e

    task_context["spock_config"] = compare_spock
    task_context["diffs"] = {}

    print(" Spock - Diff")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~")

    for n in range(1, len(compare_spock)):
        diff_key = compare_spock[0]["node"] + "/" + compare_spock[n]["node"]
        if compare_spock[0]["rep_set_info"] == compare_spock[n]["rep_set_info"]:
            task_context["diffs"][diff_key] = {
                "mismatch": False,
                "message": "Replication sets are the same between nodes "
                f"{compare_spock[0]['node']} and {compare_spock[n]['node']}",
            }
            util.message(
                f"   Replication Rules are the same for {compare_spock[0]['node']}"
                f" and {compare_spock[n]['node']}",
                p_state="success",
            )
        else:
            task_context["diffs"][diff_key] = {
                "mismatch": True,
                "message": "Replication sets are different in nodes "
                f"{compare_spock[0]['node']} and {compare_spock[n]['node']}",
            }
            ace.prRed(
                f"\u2718   Difference in Replication Rules between"
                f" {compare_spock[0]['node']} and {compare_spock[n]['node']}"
            )

    sd_task.scheduler.task_status = "COMPLETED"
    sd_task.scheduler.finished_at = datetime.now()
    sd_task.scheduler.time_taken = util.round_timedelta(
        datetime.now() - sd_start_time
    ).total_seconds()
    sd_task.scheduler.task_context = task_context
    ace_db.update_ace_task(sd_task)

    return task_context


def schema_diff_objects(sc_task: SchemaDiffTask) -> None:
    """
    Schema-diff for DDL-only. We compare tables, functions, triggers, indexes,
    and constraints.
    """

    task_context = {}
    sc_task_start_time = datetime.now()
    conns = {}

    try:
        for node in sc_task.fields.cluster_nodes:
            _, conn = sc_task.connection_pool.get_cluster_node_connection(
                node,
                client_role=(
                    sc_task.client_role
                    if config.USE_CERT_AUTH and sc_task.invoke_method == "api"
                    else None
                ),
            )
            hostname_key = node["public_ip"] + ":" + node["port"]
            conns[hostname_key] = conn
    except Exception as e:
        context = {"errors": [f"Could not connect to nodes: {str(e)}"]}
        ace.handle_task_exception(sc_task, context)
        raise e

    table_sql = """
    SELECT
        c.relname as table_name,
        a.attname as column_name,
        pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
        a.attnotnull as not_null,
        pg_get_expr(d.adbin, d.adrelid) as default_value,
        CASE
            WHEN co.contype = 'p' THEN true
            ELSE false
        END as is_primary_key
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = c.oid AND d.adnum = a.attnum)
    LEFT JOIN pg_catalog.pg_constraint co ON (co.conrelid = c.oid
        AND a.attnum = ANY(co.conkey) AND co.contype = 'p')
    WHERE n.nspname = %s
        AND c.relkind = 'r'
        AND a.attnum > 0
        AND NOT a.attisdropped
    ORDER BY c.relname, a.attnum;
    """

    function_sql = """
    SELECT
        p.proname as function_name,
        pg_get_functiondef(p.oid) as definition
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = %s
    ORDER BY p.proname;
    """

    trigger_sql = """
    SELECT
        t.tgname as trigger_name,
        pg_get_triggerdef(t.oid) as definition,
        c.relname as table_name
    FROM pg_catalog.pg_trigger t
    JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s
        AND NOT t.tgisinternal
    ORDER BY t.tgname;
    """

    index_sql = """
    SELECT
        c.relname as index_name,
        t.relname as table_name,
        pg_get_indexdef(c.oid) as definition
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
    JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
    WHERE n.nspname = %s
        AND c.relkind = 'i'
        AND t.relkind = 'r'
    ORDER BY c.relname;
    """

    constraint_sql = """
    SELECT
        co.conname as constraint_name,
        c.relname as table_name,
        pg_get_constraintdef(co.oid) as definition
    FROM pg_catalog.pg_constraint co
    JOIN pg_catalog.pg_class c ON c.oid = co.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s
        AND co.contype != 'p'
    ORDER BY co.conname;
    """

    schema_objects = {}

    # First, we simply collect all objects from each node
    for node_name, conn in conns.items():
        cur = conn.cursor(row_factory=dict_row)
        schema_objects[node_name] = {
            "tables": {},
            "functions": {},
            "triggers": {},
            "indexes": {},
            "constraints": {},
        }

        try:
            cur.execute(table_sql, (sc_task.schema_name,))
            for row in cur.fetchall():
                table_name = row["table_name"]
                if table_name not in schema_objects[node_name]["tables"]:
                    schema_objects[node_name]["tables"][table_name] = []
                schema_objects[node_name]["tables"][table_name].append(row)

            cur.execute(function_sql, (sc_task.schema_name,))
            for row in cur.fetchall():
                schema_objects[node_name]["functions"][row["function_name"]] = row[
                    "definition"
                ]

            cur.execute(trigger_sql, (sc_task.schema_name,))
            for row in cur.fetchall():
                schema_objects[node_name]["triggers"][row["trigger_name"]] = {
                    "definition": row["definition"],
                    "table_name": row["table_name"],
                }

            cur.execute(index_sql, (sc_task.schema_name,))
            for row in cur.fetchall():
                schema_objects[node_name]["indexes"][row["index_name"]] = {
                    "definition": row["definition"],
                    "table_name": row["table_name"],
                }

            cur.execute(constraint_sql, (sc_task.schema_name,))
            for row in cur.fetchall():
                schema_objects[node_name]["constraints"][row["constraint_name"]] = {
                    "definition": row["definition"],
                    "table_name": row["table_name"],
                }

        except Exception as e:
            context = {
                "errors": [f"Error collecting schema info from {node_name}: {str(e)}"]
            }
            ace.handle_task_exception(sc_task, context)
            raise e

    node_pairs = list(combinations(conns.keys(), 2))

    for node1, node2 in node_pairs:
        diff_key = f"{node1}/{node2}"
        task_context[diff_key] = {
            "tables": {"missing": {}, "different": {}},
            "functions": {"missing": {}, "different": {}},
            "triggers": {"missing": {}, "different": {}},
            "indexes": {"missing": {}, "different": {}},
            "constraints": {"missing": {}, "different": {}},
        }

        tables1 = set(schema_objects[node1]["tables"].keys())
        tables2 = set(schema_objects[node2]["tables"].keys())

        task_context[diff_key]["tables"]["missing"][node1] = list(tables2 - tables1)
        task_context[diff_key]["tables"]["missing"][node2] = list(tables1 - tables2)

        for table in tables1 & tables2:
            cols1 = {
                col["column_name"]: col
                for col in schema_objects[node1]["tables"][table]
            }
            cols2 = {
                col["column_name"]: col
                for col in schema_objects[node2]["tables"][table]
            }

            # Find columns that exist in one table but not the other
            cols_only_in_1 = set(cols1.keys()) - set(cols2.keys())
            cols_only_in_2 = set(cols2.keys()) - set(cols1.keys())

            # Columns that exist on both nodes, but may potentially have
            # differences.
            # We can also infer cols_only_in_1 and cols_only_in_2 from
            # common_cols, but simplicity ftw.
            common_cols = set(cols1.keys()) & set(cols2.keys())

            has_differences = False
            differences = {node1: [], node2: []}

            if cols_only_in_1:
                has_differences = True
                differences[node1].extend([cols1[c] for c in cols_only_in_1])

            if cols_only_in_2:
                has_differences = True
                differences[node2].extend([cols2[c] for c in cols_only_in_2])

            for col in common_cols:
                if cols1[col] != cols2[col]:
                    has_differences = True
                    differences[node1].extend([cols1[col]])
                    differences[node2].extend([cols2[col]])

            if has_differences:
                task_context[diff_key]["tables"]["different"][table] = differences

        funcs1 = set(schema_objects[node1]["functions"].keys())
        funcs2 = set(schema_objects[node2]["functions"].keys())

        task_context[diff_key]["functions"]["missing"][node1] = list(funcs2 - funcs1)
        task_context[diff_key]["functions"]["missing"][node2] = list(funcs1 - funcs2)

        for func in funcs1 & funcs2:
            if (
                schema_objects[node1]["functions"][func]
                != schema_objects[node2]["functions"][func]
            ):
                task_context[diff_key]["functions"]["different"][func] = {
                    node1: schema_objects[node1]["functions"][func],
                    node2: schema_objects[node2]["functions"][func],
                }

        trigs1 = set(schema_objects[node1]["triggers"].keys())
        trigs2 = set(schema_objects[node2]["triggers"].keys())

        task_context[diff_key]["triggers"]["missing"][node1] = list(trigs2 - trigs1)
        task_context[diff_key]["triggers"]["missing"][node2] = list(trigs1 - trigs2)

        for trig in trigs1 & trigs2:
            if (
                schema_objects[node1]["triggers"][trig]
                != schema_objects[node2]["triggers"][trig]
            ):
                task_context[diff_key]["triggers"]["different"][trig] = {
                    node1: schema_objects[node1]["triggers"][trig],
                    node2: schema_objects[node2]["triggers"][trig],
                }

        idx1 = set(schema_objects[node1]["indexes"].keys())
        idx2 = set(schema_objects[node2]["indexes"].keys())

        task_context[diff_key]["indexes"]["missing"][node1] = list(idx2 - idx1)
        task_context[diff_key]["indexes"]["missing"][node2] = list(idx1 - idx2)

        for idx in idx1 & idx2:
            if (
                schema_objects[node1]["indexes"][idx]
                != schema_objects[node2]["indexes"][idx]
            ):
                task_context[diff_key]["indexes"]["different"][idx] = {
                    node1: schema_objects[node1]["indexes"][idx],
                    node2: schema_objects[node2]["indexes"][idx],
                }

        cons1 = set(schema_objects[node1]["constraints"].keys())
        cons2 = set(schema_objects[node2]["constraints"].keys())

        task_context[diff_key]["constraints"]["missing"][node1] = list(cons2 - cons1)
        task_context[diff_key]["constraints"]["missing"][node2] = list(cons1 - cons2)

        for con in cons1 & cons2:
            if (
                schema_objects[node1]["constraints"][con]
                != schema_objects[node2]["constraints"][con]
            ):
                cons1_def = schema_objects[node1]["constraints"][con]
                cons2_def = schema_objects[node2]["constraints"][con]
                task_context[diff_key]["constraints"]["different"][con] = {
                    node1: cons1_def,
                    node2: cons2_def,
                }

        util.message(
            f"\nComparing nodes {node1} and {node2}:",
            p_state="info",
            quiet_mode=sc_task.quiet_mode,
        )

        has_differences = False
        for obj_type in ["tables", "functions", "triggers", "indexes", "constraints"]:
            missing_count = len(
                task_context[diff_key][obj_type]["missing"][node1]
            ) + len(task_context[diff_key][obj_type]["missing"][node2])
            diff_count = len(task_context[diff_key][obj_type]["different"])

            # We're only going to be printing out the summary here, just like
            # in table-diff. For more details, users will need to look at the
            # diff files.
            if missing_count > 0 or diff_count > 0:
                has_differences = True
                util.message(
                    f"  {obj_type.capitalize()}:",
                    p_state="warning",
                    quiet_mode=sc_task.quiet_mode,
                )
                if missing_count > 0:
                    util.message(
                        f"    - Missing: {missing_count}",
                        p_state="warning",
                        quiet_mode=sc_task.quiet_mode,
                    )
                if diff_count > 0:
                    util.message(
                        f"    - Different: {diff_count}",
                        p_state="warning",
                        quiet_mode=sc_task.quiet_mode,
                    )

        if not has_differences:
            util.message(
                "  No differences found",
                p_state="success",
                quiet_mode=sc_task.quiet_mode,
            )
        else:
            # TODO: Really need to figure out the dir structure here
            diff_base_path = f"diffs/{datetime.now().strftime('%Y-%m-%d')}"
            timestamp = datetime.now().strftime("%H-%M-%S")
            file_name = f"{diff_base_path}/schema_diff-{node1}-{node2}-{timestamp}.json"

            os.makedirs(os.path.dirname(file_name), exist_ok=True)

            with open(file_name, "w") as f:
                json.dump(task_context, f, indent=4)

            util.message(
                f"Schema diffs have been saved to {file_name}",
                p_state="info",
                quiet_mode=sc_task.quiet_mode,
            )

    sc_task.scheduler.task_status = "COMPLETED"
    sc_task.scheduler.finished_at = datetime.now()
    sc_task.scheduler.time_taken = util.round_timedelta(
        datetime.now() - sc_task_start_time
    ).total_seconds()
    sc_task.scheduler.task_context = {"diffs": task_context}

    ace_db.update_ace_task(sc_task)


def update_spock_exception(entry: dict, conn: psycopg.Connection) -> None:

    remote_origin = entry.get("remote_origin", None)
    remote_commit_ts = entry.get("remote_commit_ts", None)
    remote_xid = entry.get("remote_xid", None)
    status = entry.get("status", None)
    resolution_details = entry.get("resolution_details", None)
    command_counter = entry.get("command_counter", None)

    try:
        if not command_counter:
            """
            If the command_counter is not specified, we are not only updating the
            exception status in spock.exception_status, but also all exceptions
            that belong to this trio of (remote_origin, remote_commit_ts, remote_xid)
            """

            # We will first update spock.exception_status here
            update_sql = """
            UPDATE spock.exception_status
            SET
                status              = %s,
                resolution_details  = %s,
                resolved_at         = %s
            WHERE
                remote_origin         = %s
                AND remote_commit_ts  = %s
                AND remote_xid        = %s;
            """

            params = (
                status,
                json.dumps(resolution_details),
                datetime.now(),
                remote_origin,
                remote_commit_ts,
                remote_xid,
            )

            cur = conn.cursor()
            cur.execute(update_sql, params)

            # Now we will update all exceptions that belong to this trio
            update_sql = """
            UPDATE spock.exception_status_detail
            SET
                status              = %s,
                resolution_details  = %s,
                resolved_at         = %s
            WHERE
                remote_origin         = %s
                AND remote_commit_ts  = %s
                AND remote_xid        = %s;
            """

            cur.execute(update_sql, params)
            conn.commit()

        else:
            """
            If the command_counter is specified, we are only updating the
            exception status in spock.exception_status_detail
            """

            update_sql = """
            UPDATE spock.exception_status_detail
            SET
                status              = %s,
                resolution_details  = %s,
                resolved_at         = %s
            WHERE
                command_counter         = %s
                AND remote_origin       = %s
                AND remote_commit_ts    = %s
                AND remote_xid          = %s;
            """

            params = (
                status,
                json.dumps(resolution_details),
                datetime.now(),
                command_counter,
                remote_origin,
                remote_commit_ts,
                remote_xid,
            )

            cur = conn.cursor()
            cur.execute(update_sql, params)
            conn.commit()

    except Exception as e:
        raise AceException(f"Error while updating exception status: {str(e)}")


# TODO:
# - Datatypes?
# - Exception storms
def auto_repair():
    """
    This is the core function that orchestrates the auto-repair process. The
    auto-repair feature periodically polls the exception_log table, runs a
    table-diff against the remote_origin node, and attempts to repair the
    exception by making a decision on which node to use as the source of truth.
    We only need the cluster_name here, since we need to query each node for the
    exception_log and take action depending on it.

    The tricky part here is deciding which node to use as the source of truth. It
    is worth noting here that this decision, for now (the ACE 2.0 release), will
    only happen for INSERT/UPDATE/DELETE exceptions. Why? Handling auto-ddl and
    other exception types is far more complex, and will be implemented in a
    future release.

    Let us now look at the INSERT/UPDATE/DELETE scenarios:

        INSERT:
        An insert operation can error out on a subscriber if the row with the
        same primary key or unique constraint already exists on the subscriber.
        In such a scenario, the subscriber node could either be ahead or behind
        the origin node. Since we cannot make this distinction, we need to adopt
        a different strategy for choosing the source of truth.

        UPDATE:
        An update transaction coming from a remote_origin can error out on a
        subscriber if the original row to be updated is not yet available, or no
        longer exists. If it is not yet available, we can simply use the
        remote_origin node as the source of truth during repair. However, if it
        is no longer available, we can't know if the subscriber node is ahead or
        behind the origin node. Not only is this an issue, determining if the row
        is as yet unavailable, or if it is no longer available (because of a
        local or a remote delete), is non-trivial.

        DELETE:
        It's the same case for deletes. We can't ascertain if the row is as yet
        unavailable, or if it is no longer available (because of a local or a
        remote delete), or if the row was locally inserted and not yet committed.
        However, a saving grace here is that we do not need to make this
        distinction. Why? Because in the current version of Spock (4.0.1), there
        is no global ordering of transactions. It is possible for a delete
        transaction that happened after an insert/update transaction (on the
        cluster level) to be processed before the insert/update transaction, thus
        resulting in an incorrect end state for the cluster. That is why, we can
        avoid making this distinction.

    So, how do we tackle this? In all three cases above, we have uncertainties
    that prevent us from definitively determining the source of truth. However,
    we have certain functions in Spock that can help us make this decision.
    Specifically, it's the spock.xact_commit_timestamp_origin() function. This
    function returns the commit timestamp, as well as the origin node of a
    transaction. With this, here's a strategy we can employ:

        INSERT:
        We can use the spock.xact_commit_timestamp_origin() function to get the
        commit timestamp of the transaction. With this, we simply need compare
        the local commit timestamp with the origin commit timestamp to determine
        the source of truth.

        UPDATE/DELETE:
        TBD
    """

    ar_task = AutoRepairTask(
        cluster_name=config.auto_repair_config["cluster_name"],
        dbname=config.auto_repair_config["dbname"],
        poll_frequency=config.auto_repair_config["poll_frequency"],
        repair_frequency=config.auto_repair_config["repair_frequency"],
    )

    ar_task.scheduler.task_id = ace_db.generate_task_id()
    ar_task.scheduler.task_type = "AUTO_REPAIR"
    ar_task.scheduler.task_status = "RUNNING"
    ar_task.scheduler.started_at = datetime.now()
    ace_db.create_ace_task(ar_task)

    # We have already run the necessary checks, so we can proceed here.
    db, pg, node_info = cluster.load_json(ar_task.cluster_name)

    cluster_nodes = []
    combined_json = {}
    database = next(
        (db_entry for db_entry in db if db_entry["db_name"] == ar_task.dbname), None
    )

    for node in node_info:
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    get_exception_sql = """
    SELECT
        el.remote_origin,
        el.remote_commit_ts,
        el.command_counter,
        el.remote_xid,
        el.local_origin,
        el.local_commit_ts,
        el.table_schema,
        el.table_name,
        el.operation,
        el.local_tup,
        el.remote_old_tup,
        el.remote_new_tup,
        el.ddl_statement,
        el.ddl_user,
        el.error_message,
        el.retry_errored_at
    FROM
        spock.exception_log el
    INNER JOIN
        spock.exception_status_detail esd
    ON
        el.remote_origin = esd.remote_origin
        AND el.remote_commit_ts = esd.remote_commit_ts
        AND el.remote_xid = esd.remote_xid
        AND esd.status = 'PENDING';
    """

    oid_sql = """
    SELECT
        node_id,
        node_name
    FROM
        spock.node;
    """

    repair_mode_sql = "SELECT spock.repair_mode(%s);"

    update_exception_detail_sql = """
    UPDATE spock.exception_status_detail
    SET
        status              = %s,
        resolved_at         = %s,
        resolution_details  = %s
    WHERE
        remote_origin         = %s
        AND remote_commit_ts  = %s
        AND remote_xid        = %s;
    """

    # We will construct a dictionary to map OIDs to node names
    oid_to_node_name = {}
    conn_map = {}

    for node in cluster_nodes:
        try:
            _, conn = ar_task.connection_pool.get_cluster_node_connection(node)
            conn_map[node["name"]] = conn
            cur = conn_map[node["name"]].cursor(row_factory=dict_row)
            cur.execute(oid_sql)
            oid_to_node_name = {
                row["node_id"]: row["node_name"] for row in cur.fetchall()
            }
        except Exception as e:
            raise AceException(f"Error while getting OIDs: {str(e)}")

    contexts = []
    for node in cluster_nodes:

        context = {}
        context["node"] = node["name"]
        context["exception_info"] = []

        try:
            conn = conn_map[node["name"]]
            exp_cur = conn.cursor(row_factory=class_row(ExceptionLogEntry))
            sql_cur = conn.cursor()
            exp_cur.execute(get_exception_sql)
            exceptions = exp_cur.fetchall()
        except Exception as e:
            raise AceException(f"Error while getting exceptions: {str(e)}")

        # TODO: handle exception storms
        # TODO: Handle cases when the record corresponding to the exception
        # is no longer present
        for exception in exceptions:
            pkey = ace.get_key(conn, exception.table_schema, exception.table_name)

            simple_primary_key = True if len(pkey.split(",")) == 1 else False

            # We handle only INSERT exceptions for now
            if exception.operation == "INSERT":

                exception_log_entry = exception.__dict__
                exception_log_entry["pkey"] = pkey

                node_exp_context = {}
                node_exp_context["exception_log_entry"] = exception_log_entry

                # We will first get the local commit timestamp of the transaction
                # that created the record and eventually caused the exception.

                if simple_primary_key:
                    local_ts_sql = sql.SQL(
                        """
                        SELECT
                            spock.xact_commit_timestamp_origin(xmin)
                        FROM
                            {schema}.{table}
                        WHERE
                            ({pkey}) = ({pkey_value})
                        """
                    ).format(
                        schema=sql.Identifier(exception.table_schema),
                        table=sql.Identifier(exception.table_name),
                        pkey=sql.Identifier(pkey),
                        pkey_value=sql.Literal(
                            next(
                                tup["value"]
                                for tup in exception.remote_new_tup
                                if tup["attname"] == pkey
                            )
                        ),
                    )
                else:
                    local_ts_sql = sql.SQL(
                        """
                        SELECT
                            spock.xact_commit_timestamp_origin(xmin)
                        FROM
                            {schema}.{table}
                        WHERE
                            ({pkey}) = ({pkey_value})
                        """
                    ).format(
                        schema=sql.Identifier(exception.table_schema),
                        table=sql.Identifier(exception.table_name),
                        pkey=sql.SQL(", ").join(
                            sql.Identifier(col.strip()) for col in pkey.split(",")
                        ),
                        pkey_value=sql.SQL(", ").join(
                            [
                                sql.Literal(tup["value"])
                                for tup in exception.remote_new_tup
                                if tup["attname"] in pkey.split(",")
                            ]
                        ),
                    )

                local_commit_ts = None

                try:
                    sql_cur.execute(local_ts_sql)
                    local_commit_ts = parser.parse(sql_cur.fetchone()[0][0])
                    print(f"Local commit timestamp: {local_commit_ts}")
                except Exception as e:
                    raise AceException(
                        f"Error while getting local commit timestamp: {str(e)}"
                    )

                resolution_details = {}
                resolution_details["local_commit_ts"] = local_commit_ts
                resolution_details["remote_commit_ts"] = exception.remote_commit_ts

                # Now, we can compare local_commit_ts and remote_commit_ts
                # XXX: Handle equal timestamps. Maybe consult spock.node for
                # the tiebraker?
                if local_commit_ts < exception.remote_commit_ts:

                    resolution_details["action_taken"] = "INSERT_EXCEPTION_REPAIRED"
                    resolution_details["details"] = (
                        "The local record was updated"
                        + " with values from the remote origin node "
                        + f"({oid_to_node_name[exception.remote_origin]})"
                    )

                    # The local node is behind the remote origin node
                    # We will use the origin node as the source of truth

                    # The remote_new_tup will have
                    # {"value": ..., "attname": ..., "atttype": ...}
                    # We need to convert this to a tuple that can be used to update
                    # the local row

                    if simple_primary_key:
                        update_sql = sql.SQL(
                            """
                            UPDATE
                                {schema}.{table}
                            SET
                                {set_clause}
                            WHERE
                                {pkey} = {pkey_value}
                            """
                        ).format(
                            schema=sql.Identifier(exception.table_schema),
                            table=sql.Identifier(exception.table_name),
                            set_clause=sql.SQL(", ").join(
                                sql.SQL("{} = {}").format(
                                    sql.Identifier(tup["attname"]),
                                    sql.Literal(tup["value"]),
                                )
                                for tup in exception.remote_new_tup
                            ),
                            pkey=sql.Identifier(pkey),
                            pkey_value=sql.Literal(
                                next(
                                    tup["value"]
                                    for tup in exception.remote_new_tup
                                    if tup["attname"] == pkey
                                )
                            ),
                        )
                    else:
                        update_sql = sql.SQL(
                            """
                            UPDATE
                                {schema}.{table}
                            SET
                                {set_clause}
                            WHERE
                                ({pkey}) = ({pkey_value})
                            """
                        ).format(
                            schema=sql.Identifier(exception.table_schema),
                            table=sql.Identifier(exception.table_name),
                            set_clause=sql.SQL(", ").join(
                                sql.SQL("{} = {}").format(
                                    sql.Identifier(tup["attname"]),
                                    sql.Literal(tup["value"]),
                                )
                                for tup in exception.remote_new_tup
                            ),
                            pkey=sql.SQL(", ").join(
                                sql.Identifier(col.strip()) for col in pkey.split(",")
                            ),
                            pkey_value=sql.SQL(", ").join(
                                [
                                    sql.Literal(tup["value"])
                                    for tup in exception.remote_new_tup
                                    if tup["attname"] in pkey.split(",")
                                ]
                            ),
                        )

                    try:
                        sql_cur.execute(repair_mode_sql, (True,))
                        sql_cur.execute(update_sql)
                        sql_cur.execute(repair_mode_sql, (False,))
                        conn.commit()
                    except Exception as e:
                        raise AceException(
                            f"Error while updating local table"
                            f" {exception.table_schema}.{exception.table_name}: "
                            f"{str(e)}"
                        )

                    # Now, we need to update the exception status to "RESOLVED"
                    try:
                        sql_cur.execute(
                            update_exception_detail_sql,
                            (
                                "RESOLVED",
                                datetime.now(),
                                json.dumps(
                                    {
                                        "action_taken": "INSERT_EXCEPTION_REPAIRED",
                                        "details": "The local record was updated"
                                        " with values from the remote origin node "
                                        f"({oid_to_node_name[exception.remote_origin]})"
                                        " since it had a later commit timestamp"
                                        " than the local record.",
                                    }
                                ),
                                exception.remote_origin,
                                exception.remote_commit_ts,
                                exception.remote_xid,
                            ),
                        )
                        conn.commit()
                    except Exception as e:
                        raise AceException(
                            f"Error while updating exception status: {str(e)}"
                        )
                else:
                    # If the local record is ahead of the remote origin node,
                    # we will update the exception status to "RESOLVED", and
                    # log the details in the resolution_details field.
                    resolution_details["action_taken"] = "NONE"
                    resolution_details["details"] = (
                        "The local record was ahead of the"
                        " remote origin node "
                        f"({oid_to_node_name[exception.remote_origin]})"
                    )

                    try:
                        sql_cur.execute(
                            update_exception_detail_sql,
                            (
                                "RESOLVED",
                                datetime.now(),
                                json.dumps(
                                    {
                                        "action_taken": "NONE",
                                        "details": "The local record was ahead of the"
                                        " remote origin node "
                                        f"({oid_to_node_name[exception.remote_origin]})"
                                        " and was, therefore, not updated.",
                                    }
                                ),
                                exception.remote_origin,
                                exception.remote_commit_ts,
                                exception.remote_xid,
                            ),
                        )
                        conn.commit()
                    except Exception as e:
                        raise AceException(
                            f"Error while updating exception status: {str(e)}"
                        )

                node_exp_context["resolution_details"] = resolution_details
                context["exception_info"].append(node_exp_context)

        contexts.append(context)

    ar_task.scheduler.task_context = contexts
    ar_task.scheduler.task_status = "COMPLETED"
    ar_task.scheduler.finished_at = datetime.now()
    ar_task.scheduler.task_time_taken = (
        ar_task.scheduler.finished_at - ar_task.scheduler.started_at
    )

    ace_db.update_ace_task(ar_task)
