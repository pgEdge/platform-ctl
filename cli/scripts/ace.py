#  Copyright 2022-2024 PGEDGE  All rights reserved. #


"""ACE is the place of the Anti Chaos Engine"""

import ast
import json
import os
import sys
import re
import subprocess
from datetime import datetime
import logging
from itertools import chain

import ace_core
import ace_daemon
import fire
import psycopg
from psycopg.rows import dict_row
from psycopg import sql

import cluster
import util
import ace_db
import ace_config as config
from ace_data_models import (
    MerkleTreeTask,
    RepsetDiffTask,
    SchemaDiffTask,
    SpockDiffTask,
    TableDiffTask,
    TableRepairTask,
)
import ace_cli
from ace_auth import ConnectionPool
from ace_exceptions import AceException


"""
CAUTION:
Do not declare any variables in the global scope of this file.
This causes mpire to crash on macOS and Windows because of the
freeze_support() issue.

https://stackoverflow.com/a/60693016/7170797
"""


def prCyan(skk):
    print("\033[96m {}\033[00m".format(skk))


def prRed(skk):
    print("\033[91m {}\033[00m".format(skk))


def get_dump_file_name(p_prfx, p_schm, p_base_dir="/tmp"):
    return p_base_dir + os.sep + p_prfx + "-" + p_schm + ".sql"


def write_pg_dump(p_ip, p_db, p_port, p_prfx, p_schm, p_base_dir="/tmp"):
    out_file = get_dump_file_name(p_prfx, p_schm, p_base_dir)
    try:
        cmd = f"pg_dump -s -n {p_schm} -h {p_ip} -p {p_port} -d {p_db} > {out_file}"
        os.system(cmd)
    except Exception as e:
        util.exit_exception(e)
    return out_file


"""
Accepts a connection object and returns the version of spock installed

@param: conn - connection object
@return: float - version of spock installed

"""


def get_spock_version(conn):
    data = []
    sql = "SELECT spock.spock_version();"
    try:
        c = conn.cursor()
        c.execute(sql)
        data = c.fetchone()
        if data:
            """
            Spock returns 4.0.0 or something similar.
            We are interested only in the first part of the version
            """
            data = data[0].split(".")[0]
            return float(data)
        else:
            util.exit_message("Could not get spock version from the database")
    except Exception as e:
        util.exit_message("Error in get_spock_version(): " + str(e))

    return 0.0


def fix_schema(diff_file, sql1, sql2):
    newtable = False
    with open(diff_file) as diff_list:
        for i in diff_list.readlines():
            if re.search(r"\\,", i):
                # TODO: Fix this
                # linenum = i.split(",")[0]
                pass
            elif re.search(r"^< CREATE.", i):
                newtable = True
                print(i.replace("<", ""))
            elif re.search(r"^< ALTER.", i):
                print(i.replace("<", ""))
            elif re.search(r"^> CREATE TABLE.", i):
                print(
                    " DROP TABLE " + i.replace("> CREATE TABLE ", "").replace(" (", ";")
                )
            elif newtable:
                print(i.replace("<", ""))
                if re.search(r".;$", i):
                    newtable = False
            else:
                continue
    return 1


def get_row_count(p_con, p_schema, p_table):
    sql = f'SELECT count(*) FROM {p_schema}."{p_table}"'

    try:
        cur = p_con.cursor()
        cur.execute(sql)
        r = cur.fetchone()
        cur.close()
    except Exception as e:
        util.exit_message("Error in get_row_count():\n" + str(e), 1)

    if not r:
        return 0

    rows = int(r[0])

    return rows


def get_cols(p_con, p_schema, p_table):
    sql = """
    SELECT
        column_name
    FROM
        information_schema.columns
    WHERE
        table_schema = %s
        AND table_name = %s
    """

    try:
        cur = p_con.cursor()
        cur.execute(
            sql,
            [
                p_schema,
                p_table,
            ],
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        util.exit_message("Error in get_cols():\n" + str(e), 1)

    if not rows:
        return None

    col_lst = []
    for row in rows:
        col_lst.append(str(row[0]))

    return ",".join(col_lst)


def get_key(p_con, p_schema, p_table):
    sql = """
    SELECT
        kcu.column_name
    FROM
        information_schema.table_constraints tc
    JOIN
        information_schema.key_column_usage kcu
    ON
        tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = %s
        AND tc.table_name = %s
    """

    try:
        cur = p_con.cursor()
        cur.execute(
            sql,
            [
                p_schema,
                p_table,
            ],
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        util.exit_message("Error in get_key():\n" + str(e), 1)

    if not rows:
        return None

    key_lst = []
    for row in rows:
        key_lst.append(str(row[0]))

    return ",".join(key_lst)


def parse_nodes(nodes, quiet_mode=False) -> list:
    node_list = []
    if type(nodes) is str and nodes != "all":
        node_list = [s.strip() for s in nodes.split(",")]
    elif type(nodes) is not str:
        node_list = nodes

    if nodes != "all":
        rep_check = set(node_list)
        if len(rep_check) < len(node_list):
            util.message(
                "Ignoring duplicate node names",
                p_state="warning",
                quiet_mode=quiet_mode,
            )
            node_list = list(rep_check)

    return node_list


def parse_skip_list(skip_list: str, skip_file: str) -> list:
    """
    Utility function to parse skip_list and skip_file.
    If both skip_list and skip_file are provided, we'll use a
    union of the two.
    """

    table_regex = re.compile(r"^[a-z]+\.[a-zA-Z0-9_]+$")
    skip_tables_list = []

    if (skip_list and type(skip_list) is not str) or (
        skip_file and type(skip_file) is not str
    ):
        raise AceException("Invalid value type for skip_tables")

    if skip_file and not os.path.exists(skip_file):
        raise AceException(f"Skip file {skip_file} not found")

    if skip_list:
        raw_list = skip_list.split(",")
        for item in raw_list:
            if table_regex.match(item):
                skip_tables_list.append(item)
            else:
                util.message(f"Ignoring invalid table name {item}", p_state="warning")

    if skip_file:
        with open(skip_file, "r") as f:
            for line in f:
                if table_regex.match(line.strip()):
                    skip_tables_list.append(line.strip())
                else:
                    util.message(
                        f"Ignoring invalid table name {line.strip()}", p_state="warning"
                    )

    return skip_tables_list


def parse_bool_field(field_name: str, field_value) -> bool:
    if type(field_value) is int:
        if field_value < 0 or field_value > 1:
            raise AceException(f"{field_name} should be True (1) or False (0)")
        field_value = bool(field_value)
    elif type(field_value) is str:
        if field_value in ["True", "true", "1", "t"]:
            field_value = True
        elif field_value in ["False", "false", "0", "f"]:
            field_value = False
        else:
            raise AceException(f"Invalid value for {field_name}")
    elif type(field_value) is not bool:
        raise AceException(f"{field_name} should be True (1) or False (0)")

    return field_value


def get_col_types(conn, table_name):
    """
    Here we are grabbing the name and data type of each row from table from the
    given conn. Using this complex query instead of a simpler one since this
    gives the correct name of non standard datatypes (i.e. vectors from Vector
    and geometries from PostGIS)
    """

    psql_qry = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
        FROM
            pg_catalog.pg_attribute a
        JOIN
            pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN
            pg_catalog.pg_type t ON a.atttypid = t.oid
        LEFT JOIN
            pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE
            c.relname = %s
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY
            a.attnum;
    """

    try:
        cur = conn.cursor()
        cur.execute(psql_qry, (table_name,))
        table_types = {col_name: col_type for col_name, col_type in cur.fetchall()}
    except Exception:
        table_types = dict()

    finally:
        conn.commit()
        if not table_types:
            raise AceException("Error: could not fetch column types")

    return table_types


def check_cluster_exists(cluster_name, base_dir="cluster"):
    cluster_dir = base_dir + "/" + str(cluster_name)
    return True if os.path.exists(cluster_dir) else False


def check_user_privileges(conn, username, schema, table, required_privileges=[]):
    """
    In most cases, the ace user provisioned in production will have limited access
    to the tables. We will use this function to check for specific privileges when
    an ACE module is invoked.
    - table-diff, repset-diff, table-rerun need SELECT
    - table-repair needs SELECT, INSERT, UPDATE, DELETE
    - table-repair with --upsert-only needs SELECT, INSERT, UPDATE
    """

    # This query will give us the privilege type and whether the user has that
    # specific privilege or not. We use quote_ident to properly handle
    # case-sensitive identifiers.
    query = """
    WITH params AS (
        SELECT %s::text AS username,
               %s::text AS schema_name,
               %s::text AS table_name
    ),
    table_check AS (
        SELECT c.relname as table_name, n.nspname as table_schema
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = (SELECT schema_name FROM params)
          AND c.relname = (SELECT table_name FROM params)
    )
    SELECT
        CASE
            WHEN EXISTS (SELECT 1 FROM table_check)
            THEN has_table_privilege(
                (SELECT username FROM params),
                (
                    SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)
                    FROM table_check
                ),
                'SELECT'
            )
            ELSE FALSE
        END AS table_select,
        has_schema_privilege(
            (SELECT username FROM params),
            (SELECT schema_name FROM params),
            'CREATE'
        ) AS table_create,
        CASE
            WHEN EXISTS (SELECT 1 FROM table_check)
            THEN has_table_privilege(
                (SELECT username FROM params),
                (
                    SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)
                    FROM table_check
                ),
                'INSERT'
            )
            ELSE FALSE
        END AS table_insert,
        CASE
            WHEN EXISTS (SELECT 1 FROM table_check)
            THEN has_table_privilege(
                (SELECT username FROM params),
                (
                    SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)
                    FROM table_check
                ),
                'UPDATE'
            )
            ELSE FALSE
        END AS table_update,
        CASE
            WHEN EXISTS (SELECT 1 FROM table_check)
            THEN has_table_privilege(
                (SELECT username FROM params),
                (
                    SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)
                    FROM table_check
                ),
                'DELETE'
            )
            ELSE FALSE
        END AS table_delete,
        has_table_privilege(
            (SELECT username FROM params),
            'information_schema.columns',
            'SELECT'
        ) AS columns_select,
        has_table_privilege(
            (SELECT username FROM params),
            'information_schema.table_constraints',
            'SELECT'
        ) AS table_constraints_select,
        has_table_privilege(
            (SELECT username FROM params),
            'information_schema.key_column_usage',
            'SELECT'
        ) AS key_column_usage_select;
    """

    if required_privileges:
        # Transform required_privileges to column names we use in the query
        required_privileges = [f"table_{p.lower()}" for p in required_privileges]

    cur = conn.cursor(row_factory=dict_row)
    cur.execute(query, (username, schema, table))
    results = cur.fetchone()

    if not required_privileges:
        return all(results.values()), {k: v for k, v in results.items() if not v}
    else:
        return all([results[p] for p in required_privileges]), {
            k: v for k, v in results.items() if not v
        }


# Looks through a table to ensure that no `bytea` column is too large
# Returns (true, _) if it is ok to proceed, (false, rowname) otherwise
def check_column_size(conn_list: list, task: TableDiffTask) -> tuple[bool, str]:
    # Gets byte size from each bytea in each connection
    byte_sql = (
        'SELECT AVG(pg_column_size("{c_name}")) AS avg_size_in_bytes '
        'FROM "{s_name}"."{t_name}";'
    )

    try:
        for conn in conn_list:
            cur = conn.cursor()
            hostname = conn.info.host
            port = conn.info.port
            col_types_key = f"{hostname}:{port}"
            table_types = task.fields.col_types[col_types_key]
            for col_name, col_type in table_types.items():
                if "bytea" in col_type:
                    cur.execute(
                        byte_sql.format(
                            c_name=col_name,
                            s_name=task.fields.l_schema,
                            t_name=task.fields.l_table,
                        )
                    )
                    size = cur.fetchone()[0]
                    # If max size greater than (1 MB = 10^6 B)?? return false
                    if size > 10**6:
                        return False, col_name
    except Exception as e:
        context = {
            "errors": [f"Could not read average byte size of some column: {str(e)}"]
        }
        handle_task_exception(task, context)
        raise e

    # Everything ok
    return True, ""


def check_diff_file_format(diff_file_path: str, task) -> dict:
    """
    If the diff-file is not a valid json, then we throw an error message and exit.
    However, if the diff-file is a valid json, it's a slightly trickier case.
    Our diff-file is a json of the form:
    {
        "diffs": {
            "node1/node2": {
                "node1": [{"col1": "val1", "col2": "val2", ...}, ...],
                "node2": [{"col1": "val1", "col2": "val2", ...}, ...]
            },
            "node1/node3": {
                "node1": [{"col1": "val1", "col2": "val2", ...}, ...],
                "node3": [{"col1": "val1", "col2": "val2", ...}, ...]
            }
        },
        "summary": {...}

    }

    We need to make sure that the root-level keys are of the form "node1/node2" and
    that the inner keys have the corresponding node names. E.g., if the root-level key
    is "node1/node2", then the inner keys should be "node1" and "node2".

    A simple way we achieve this is by iterating over the root keys and checking if the
    inner keys are contained in the list when the root key is split by "/". If not, we
    throw an error message and exit.

    TODO: It might be possible that the diff file has different cols compared to the
    target table. We need to handle this case.
    """
    try:
        diff_json = json.loads(open(diff_file_path, "r").read())
    except Exception as e:
        context = {"errors": [f"Could not load diff file as JSON: {str(e)}"]}
        handle_task_exception(task, context)
        raise e

    try:
        if any(
            [
                set(list(diff_json["diffs"][k].keys())) != set(k.split("/"))
                for k in diff_json["diffs"].keys()
            ]
        ):
            raise AceException("Contents of diff file improperly formatted")

        diff_json = {
            node_pair: {
                node: [{key: val for key, val in row.items()} for row in rows]
                for node, rows in nodes_data.items()
            }
            for node_pair, nodes_data in diff_json["diffs"].items()
        }
    except Exception as e:
        context = {"errors": [f"Could not read diff file: {str(e)}"]}
        handle_task_exception(task, context)
        raise e

    return diff_json


def convert_pg_type_to_json(item: str, type: str):
    """
    Converts a value from a postgres column to a json-compatible type.
    """
    # TODO: Need to revisit this.
    try:
        # List of types that should be treated as strings
        string_types = [
            "char",
            "text",
            "time",
            "bytea",
            "uuid",
            "date",
            "timestamp",
            "interval",
            "inet",
            "macaddr",
            "xml",
            "money",
            "point",
            "line",
            "polygon",
        ]

        # Types that can be directly represented in JSON
        json_compatible_types = [
            "json",
            "jsonb",
            "boolean",
            "integer",
            "bigint",
            "smallint",
            "numeric",
            "real",
            "double precision",
        ]

        type_lower = type.lower()

        if not item or item == "" or item.lower() == "null" or item.lower() == "none":
            return None
        elif "[]" in type_lower:
            return ast.literal_eval(item)
        elif any(s in type_lower for s in json_compatible_types):
            # For JSON-compatible types, parse them using AST
            return ast.literal_eval(item)
        elif any(s in type_lower for s in string_types):
            return item
        else:
            # Default to treating as string if type is unknown
            return item

    except Exception as e:
        raise AceException(
            f"Could not convert value {item} to {type} while writing to json: {e}"
        )


def convert_json_to_pg_type(rows, cols_list, col_types) -> list[tuple]:
    """
    Converts a value from a json column to a postgres-compatible type.

    We had previously converted all rows to strings for computing set differences.
    We now need to convert them back to their original types before upserting.
    psycopg3 will internally handle type conversions from lists/arrays to their
    respective types in Postgres.

    So, if an element in a row is a list or a json that is represented as:
    '{"key1": "val1", "key2": "val2"}' or '[1, 2, 3]', we need to use the
    abstract syntax tree module to convert them back to their original types.
    ast.literal_eval() will give us {'key1': 'val1', 'key2': 'val2'} and
    [1, 2, 3] respectively.
    """

    # List of types that should be treated as strings
    string_types = [
        "char",
        "text",
        "time",
        "bytea",
        "uuid",
        "date",
        "timestamp",
        "interval",
        "inet",
        "macaddr",
        "xml",
        "money",
        "point",
        "line",
        "polygon",
        "vector",
    ]

    # Types that can be directly represented in JSON
    json_compatible_types = [
        "json",
        "jsonb",
        "boolean",
        "integer",
        "bigint",
        "smallint",
        "numeric",
        "real",
        "double precision",
    ]

    repair_tuples = []

    for row in rows:
        modified_row = tuple()
        for col_name in cols_list:
            col_type = col_types[col_name]
            elem = str(row[col_name])

            try:
                type_lower = col_type.lower()

                if (
                    not elem
                    or elem == ""
                    or elem.lower() == "null"
                    or elem.lower() == "none"
                ):
                    modified_row += (None,)
                elif "[]" in type_lower:
                    modified_row += (ast.literal_eval(elem),)
                elif any(s in type_lower for s in string_types):
                    if type_lower == "bytea":
                        modified_row += (bytes.fromhex(elem),)
                    else:
                        modified_row += (elem,)
                elif any(s in type_lower for s in json_compatible_types):
                    item = ast.literal_eval(elem)
                    if type_lower == "jsonb" or type_lower == "json":
                        item = json.dumps(item)
                    modified_row += (item,)
                else:
                    modified_row += (elem,)

            except (ValueError, SyntaxError):
                # If conversion fails, use the original value
                modified_row += (elem,)

        repair_tuples.append(modified_row)

    return repair_tuples


def write_diffs_json(td_task, diff_dict, col_types, quiet_mode=False):
    """
    All diff runs from ACE will be stored in diffs/<date>/diffs_<time>.json
    Each day will have its own directory and each run will have its own file
    that indicates the time of the run.
    """
    now = datetime.now()
    dirname = now.strftime("%Y-%m-%d")
    diff_file_suffix = now.strftime("%H%M%S") + f"{now.microsecond // 1000:03d}"
    diff_filename = "diffs_" + diff_file_suffix + ".json"

    if not os.path.exists("diffs"):
        os.mkdir("diffs")

    dirname = os.path.join("diffs", dirname)

    if not os.path.exists(dirname):
        os.mkdir(dirname)

    filename = os.path.join(dirname, diff_filename)

    # Since we have column types for all nodes, we can just take the first one
    col_types = next(iter(col_types.values()))

    # Converts diff so that values are correct json type
    write_dict = {
        node_pair: {
            node: [
                {
                    key: convert_pg_type_to_json(val, col_types[key])
                    for key, val in row.items()
                }
                for row in rows
            ]
            for node, rows in nodes_data.items()
        }
        for node_pair, nodes_data in diff_dict.items()
    }

    # Depending on whether we have simple or a composite primary key,
    # we will sort the diffs.

    for node_pair in write_dict.keys():
        for node in write_dict[node_pair].keys():
            if len(td_task.fields.key.split(",")) == 1:
                write_dict[node_pair][node] = sorted(
                    write_dict[node_pair][node], key=lambda x: x[td_task.fields.key]
                )
            else:
                write_dict[node_pair][node] = sorted(
                    write_dict[node_pair][node],
                    key=lambda x: tuple(x[k] for k in td_task.fields.key.split(",")),
                )

    output_json = dict({"diffs": write_dict})
    output_json["summary"] = {
        "task_id": td_task.scheduler.task_id,
        "schema_name": td_task._table_name.split(".")[0],
        "table_name": td_task._table_name.split(".")[1],
        "nodes": td_task._nodes,
        "block_rows": td_task.block_rows,
        "max_cpu_ratio": td_task.max_cpu_ratio,
        "batch_size": td_task.batch_size,
        "start_time": td_task.scheduler.started_at,
        "end_time": td_task.scheduler.finished_at,
        "time_taken": td_task.scheduler.time_taken,
        "total_rows_checked": td_task.scheduler.task_context["total_rows"],
        "diff_count": td_task.diff_summary,
    }

    if not quiet_mode:
        with open(filename, "w") as f:
            f.write(json.dumps(output_json, default=str, indent=2))
    else:
        print(json.dumps(output_json, default=str))

    util.message(
        f"Diffs written out to" f" {util.set_colour(filename, 'blue')}",
        p_state="info",
        quiet_mode=quiet_mode,
    )
    return filename


# TODO: Come up with better naming convention for diff files
def write_diffs_csv(diff_dict):
    import pandas as pd

    """
    All diff runs from ACE will be stored in diffs/<date>/diffs_<time>.json
    Each day will have its own directory and each run will have its own file
    that indicates the time of the run.
    """
    now = datetime.now()
    dirname = now.strftime("%Y-%m-%d")

    if not os.path.exists("diffs"):
        os.mkdir("diffs")

    dirname = os.path.join("diffs", dirname)

    if not os.path.exists(dirname):
        os.mkdir(dirname)

    for node_pair in diff_dict.keys():
        diff_file_suffix = now.strftime("%H%M%S") + f"{now.microsecond // 1000:03d}"
        node1, node2 = node_pair.split("/")
        node_str = f"{node1}_{node2}"
        diff_file_name = f"{node_str}_" + diff_file_suffix + ".diff"
        diff_file_path = os.path.join(dirname, diff_file_name)
        t1_file_name = node1 + ".csv"
        t2_file_name = node2 + ".csv"

        t1_write_path = os.path.join(dirname, t1_file_name)
        t2_write_path = os.path.join(dirname, t2_file_name)

        df1 = pd.DataFrame.from_dict(diff_dict[node_pair][node1])
        df2 = pd.DataFrame.from_dict(diff_dict[node_pair][node2])

        df1.to_csv(t1_write_path, header=True, index=False)
        df2.to_csv(t2_write_path, header=True, index=False)

        cmd = f"diff -u {t1_write_path} {t2_write_path} | ydiff > {diff_file_path}"
        subprocess.check_output(cmd, shell=True)

        # Delete the temporary files
        os.remove(t1_write_path)
        os.remove(t2_write_path)

        util.message(
            f"DIFFS BETWEEN {util.set_colour(node1, 'blue')}"
            f" AND {util.set_colour(node2, 'blue')}: {diff_file_path}",
            p_state="info",
        )


def validate_merkle_tree_inputs(mtree_task: MerkleTreeTask) -> None:
    """
    Validates the basic inputs for a merkle tree task without establishing connections.
    Raises AceException if validation fails.
    """
    if not mtree_task.cluster_name or not mtree_task._table_name:
        raise AceException("cluster_name and table_name are required arguments")

    if type(mtree_task.block_rows) is str:
        try:
            mtree_task.block_rows = int(mtree_task.block_rows)
        except Exception:
            raise AceException("Invalid values for ACE_BLOCK_ROWS")
    elif type(mtree_task.block_rows) is not int:
        raise AceException("Invalid value type for ACE_BLOCK_ROWS")

    # Capping max block size here to prevent the hash function from taking forever
    if mtree_task.block_rows > config.MAX_MTREE_BLOCK_SIZE:
        raise AceException(f"Block row size should be <= {config.MAX_MTREE_BLOCK_SIZE}")
    if mtree_task.block_rows < config.MIN_MTREE_BLOCK_SIZE:
        raise AceException(f"Block row size should be >= {config.MIN_MTREE_BLOCK_SIZE}")

    if type(mtree_task.max_cpu_ratio) is int:
        mtree_task.max_cpu_ratio = float(mtree_task.max_cpu_ratio)
    elif type(mtree_task.max_cpu_ratio) is str:
        try:
            mtree_task.max_cpu_ratio = float(mtree_task.max_cpu_ratio)
        except Exception:
            raise AceException("Invalid values for ACE_MAX_CPU_RATIO")
    elif type(mtree_task.max_cpu_ratio) is not float:
        raise AceException("Invalid value type for ACE_MAX_CPU_RATIO")

    if mtree_task.max_cpu_ratio > 1.0 or mtree_task.max_cpu_ratio < 0.0:
        raise AceException(
            "Invalid value range for ACE_MAX_CPU_RATIO or --max_cpu_ratio"
        )

    node_list = []
    try:
        node_list = parse_nodes(mtree_task._nodes)
    except ValueError as e:
        raise AceException(
            "Nodes should be a comma-separated list of nodenames "
            + f'\n\tE.g., --nodes="n1,n2". Error: {e}'
        )

    if len(node_list) > 3:
        raise AceException(
            "table-diff currently supports up to a three-way table comparison"
        )

    if mtree_task._nodes != "all" and len(node_list) == 1:
        raise AceException("table-diff needs at least two nodes to compare")

    found = check_cluster_exists(mtree_task.cluster_name)
    if found:
        util.message(
            f"Cluster {mtree_task.cluster_name} exists",
            p_state="success",
            quiet_mode=mtree_task.quiet_mode,
        )
    else:
        raise AceException(f"Cluster {mtree_task.cluster_name} not found")

    nm_lst = mtree_task._table_name.split(".")
    if len(nm_lst) != 2:
        raise AceException(
            f"TableName {mtree_task._table_name} must be of form" " 'schema.table_name'"
        )
    l_schema = nm_lst[0]
    l_table = nm_lst[1]

    db, pg, node_info = cluster.load_json(mtree_task.cluster_name)

    cluster_nodes = []
    database = {}

    if mtree_task._dbname:
        for db_entry in db:
            if db_entry["db_name"] == mtree_task._dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{mtree_task._dbname}' "
            + f"not found in cluster '{mtree_task.cluster_name}'"
        )

    # Combine db and cluster_nodes into a single json
    for node in node_info:
        if node_list and node["name"] not in node_list:
            continue
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    if not node_list:
        node_list = [node["name"] for node in cluster_nodes]

    if mtree_task._nodes == "all" and len(cluster_nodes) > 3:
        raise AceException("Table-diff only supports up to three way comparison")

    if mtree_task._nodes != "all" and len(node_list) > 1:
        for n in node_list:
            if not any(filter(lambda x: x["name"] == n, cluster_nodes)):
                raise AceException("Specified nodenames not present in cluster")

    # Store basic task information
    mtree_task.fields.l_schema = l_schema
    mtree_task.fields.l_table = l_table
    mtree_task.fields.node_list = node_list
    mtree_task.fields.database = database
    mtree_task.fields.cluster_nodes = cluster_nodes


def validate_table_diff_inputs(td_task: TableDiffTask) -> None:
    """
    Validates the basic inputs for a table diff task without establishing connections.
    Raises AceException if validation fails.
    """
    if not td_task.cluster_name or not td_task._table_name:
        raise AceException("cluster_name and table_name are required arguments")

    if type(td_task.block_rows) is str:
        try:
            td_task.block_rows = int(td_task.block_rows)
        except Exception:
            raise AceException("Invalid values for ACE_BLOCK_ROWS")
    elif type(td_task.block_rows) is not int:
        raise AceException("Invalid value type for ACE_BLOCK_ROWS")

    # Capping max block size here to prevent the hash function from taking forever
    if td_task.block_rows > config.MAX_DIFF_BLOCK_SIZE:
        raise AceException(f"Block row size should be <= {config.MAX_DIFF_BLOCK_SIZE}")
    if td_task.block_rows < config.MIN_DIFF_BLOCK_SIZE:
        raise AceException(f"Block row size should be >= {config.MIN_DIFF_BLOCK_SIZE}")

    if type(td_task.max_cpu_ratio) is int:
        td_task.max_cpu_ratio = float(td_task.max_cpu_ratio)
    elif type(td_task.max_cpu_ratio) is str:
        try:
            td_task.max_cpu_ratio = float(td_task.max_cpu_ratio)
        except Exception:
            raise AceException("Invalid values for ACE_MAX_CPU_RATIO")
    elif type(td_task.max_cpu_ratio) is not float:
        raise AceException("Invalid value type for ACE_MAX_CPU_RATIO")

    if td_task.max_cpu_ratio > 1.0 or td_task.max_cpu_ratio < 0.0:
        raise AceException(
            "Invalid value range for ACE_MAX_CPU_RATIO or --max_cpu_ratio"
        )

    if td_task.output not in ["csv", "json", "html"]:
        raise AceException(
            "table-diff currently supports only csv, json and html output formats"
        )

    node_list = []
    try:
        node_list = parse_nodes(td_task._nodes)
    except ValueError as e:
        raise AceException(
            "Nodes should be a comma-separated list of nodenames "
            + f'\n\tE.g., --nodes="n1,n2". Error: {e}'
        )

    if len(node_list) > 3:
        raise AceException(
            "table-diff currently supports up to a three-way table comparison"
        )

    if td_task._nodes != "all" and len(node_list) == 1:
        raise AceException("table-diff needs at least two nodes to compare")

    found = check_cluster_exists(td_task.cluster_name)
    if found:
        util.message(
            f"Cluster {td_task.cluster_name} exists",
            p_state="success",
            quiet_mode=td_task.quiet_mode,
        )
    else:
        raise AceException(f"Cluster {td_task.cluster_name} not found")

    nm_lst = td_task._table_name.split(".")
    if len(nm_lst) != 2:
        raise AceException(
            f"TableName {td_task._table_name} must be of form" " 'schema.table_name'"
        )
    l_schema = nm_lst[0]
    l_table = nm_lst[1]

    db, pg, node_info = cluster.load_json(td_task.cluster_name)

    cluster_nodes = []
    database = {}

    if td_task._dbname:
        for db_entry in db:
            if db_entry["db_name"] == td_task._dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{td_task._dbname}' "
            + f"not found in cluster '{td_task.cluster_name}'"
        )

    # Combine db and cluster_nodes into a single json
    for node in node_info:
        if node_list and node["name"] not in node_list:
            continue
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    if not node_list:
        node_list = [node["name"] for node in cluster_nodes]

    if td_task._nodes == "all" and len(cluster_nodes) > 3:
        raise AceException("Table-diff only supports up to three way comparison")

    if td_task._nodes != "all" and len(node_list) > 1:
        for n in node_list:
            if not any(filter(lambda x: x["name"] == n, cluster_nodes)):
                raise AceException("Specified nodenames not present in cluster")

    # Store basic task information
    td_task.fields.l_schema = l_schema
    td_task.fields.l_table = l_table
    td_task.fields.node_list = node_list
    td_task.fields.database = database
    td_task.fields.cluster_nodes = cluster_nodes


def table_diff_checks(
    td_task: TableDiffTask, skip_validation: bool = False
) -> TableDiffTask:
    """
    Validates and prepares a table diff task.

    This function performs full validation including database connections
    and privilege checks. It should be called in the worker process.

    Returns:
        The validated and prepared task
    """
    # First do basic validation
    if not skip_validation:
        validate_table_diff_inputs(td_task)

    # Now do connection-specific validation
    cols = None
    key = None
    conn_params = []
    conn_list = []
    host_map = {}
    required_privileges = ["SELECT"]
    l_schema = td_task.fields.l_schema
    l_table = td_task.fields.l_table

    try:
        for node_info in td_task.fields.cluster_nodes:
            hostname = node_info["name"]
            host_ip = node_info["public_ip"]
            user = node_info["db_user"]
            port = node_info.get("port", 5432)

            if node_info["name"] in td_task.fields.node_list:
                params, conn = td_task.connection_pool.get_cluster_node_connection(
                    node_info,
                    client_role=(
                        td_task.client_role if td_task.invoke_method == "api" else None
                    ),
                )
            else:
                continue

            curr_cols = get_cols(conn, l_schema, l_table)
            curr_key = get_key(conn, l_schema, l_table)

            if not curr_cols:
                raise AceException(
                    f"Table '{td_task._table_name}' not found on {hostname}"
                    ", or the current user does not have adequate privileges"
                )
            if not curr_key:
                raise AceException(f"No primary key found for '{td_task._table_name}'")

            if (not cols) and (not key):
                cols = curr_cols
                key = curr_key

            if (curr_cols != cols) or (curr_key != key):
                raise AceException("Table schemas don't match")

            cols = curr_cols
            key = curr_key

            col_types = get_col_types(conn, l_table)
            col_types_key = f"{host_ip}:{port}"

            if not td_task.fields.col_types:
                td_task.fields.col_types = {}

            td_task.fields.col_types[col_types_key] = col_types

            authorised, missing_privileges = check_user_privileges(
                conn,
                user,
                l_schema,
                l_table,
                required_privileges,
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
                f'User "{user}" does not have the necessary privileges'
                f" to run {', '.join(missing_privs)} "
                f'on table "{l_schema}.{l_table}" '
                f'on node "{hostname}"'
            )

            if not authorised:
                raise AceException(exception_msg)

            conn_list.append(conn)
            conn_params.append(params)
            host_map[host_ip + ":" + str(port)] = hostname

            # TODO:
            # 1. Add filter information to the task context in the scheduler.
            # 2. Keep track of the original table name

            # Now we will create a view for the table filter if necessary
            if td_task.table_filter:

                # We're going to be using parameterised queries here
                # to prevent SQL injections.
                view_sql = sql.SQL(
                    """
                    CREATE VIEW {view_name} AS
                    SELECT * FROM {l_schema}.{l_table}
                    WHERE {where_clause}
                """
                ).format(
                    view_name=sql.Identifier(
                        f"{td_task.scheduler.task_id}_{l_table}_filtered"
                    ),
                    l_schema=sql.Identifier(l_schema),
                    l_table=sql.Identifier(l_table),
                    where_clause=sql.SQL(td_task.table_filter),
                )
                cur = conn.cursor()
                cur.execute(view_sql)
                conn.commit()

                # Now, we need to check if the view actually has any rows
                view_sql = sql.SQL("SELECT COUNT(*) FROM {view_name}").format(
                    view_name=sql.Identifier(
                        f"{td_task.scheduler.task_id}_{l_table}_filtered"
                    ),
                )
                cur = conn.cursor()
                cur.execute(view_sql)
                row_count = cur.fetchone()[0]
                if row_count == 0:
                    raise AceException("Table filter produced no rows")

    except Exception as e:
        raise e

    util.message(
        "Connections successful to nodes in cluster",
        p_state="success",
        quiet_mode=td_task.quiet_mode,
    )

    # Psycopg connection objects cannot be pickled easily,
    # so, we send the connection parameters instead
    td_task.fields.conn_params = conn_params
    td_task.fields.host_map = host_map
    td_task.fields.cols = cols
    td_task.fields.key = key

    if td_task.fields.col_types and len(td_task.fields.col_types) > 1:
        ref_node = list(td_task.fields.col_types.keys())[0]
        ref_types = td_task.fields.col_types[ref_node]

        for node, types in td_task.fields.col_types.items():
            if types != ref_types:
                mismatched_cols = {
                    col: (ref_types[col], types[col])
                    for col in ref_types
                    if col in types and types[col] != ref_types[col]
                }
                util.message(
                    "Warning: Column types mismatch detected between"
                    f" {ref_node} and {node}:\n"
                    + "\n".join(
                        [
                            f"  Column '{col}': {ref_node}={t1}, {node}={t2}"
                            for col, (t1, t2) in mismatched_cols.items()
                        ]
                    ),
                    p_state="warning",
                    quiet_mode=td_task.quiet_mode,
                )

    util.message(
        f"Table {td_task._table_name} is comparable across nodes",
        p_state="success",
        quiet_mode=td_task.quiet_mode,
    )

    # Checks to see if there is a `bytea` dataype that is too large
    byte_check, byte_row_name = check_column_size(conn_list, td_task)
    if not byte_check:
        raise AceException(
            f"Refusing to perform table-diff. Data in column {byte_row_name} "
            f"of table {td_task._table_name} is larger than 1 MB"
        )

    if td_task.diff_file_path:
        check_diff_file_format(td_task.diff_file_path, td_task)

    # Finally, we will replace the table name with the view name if necessary
    if td_task.table_filter:
        td_task.fields.l_table = (
            f"{td_task.scheduler.task_id}_{td_task.fields.l_table}_filtered"
        )

    return td_task


def merkle_tree_checks(
    mtree_task: MerkleTreeTask, skip_validation: bool = False
) -> None:
    """
    Checks if a table is 'diffable' for a merkle tree.

    TODO: This is a temporary function since all these checks are already a part
    of the table-diff checks. Will clean this up eventually.

    Returns:
        The validated and prepared task
    """

    # First do basic validation
    if not skip_validation:
        validate_merkle_tree_inputs(mtree_task)

    # Now do connection-specific validation
    cols = None
    key = None
    conn_params = []
    conn_list = []
    host_map = {}
    required_privileges = ["SELECT"]
    l_schema = mtree_task.fields.l_schema
    l_table = mtree_task.fields.l_table

    try:
        for node_info in mtree_task.fields.cluster_nodes:
            hostname = node_info["name"]
            host_ip = node_info["public_ip"]
            user = node_info["db_user"]
            port = node_info.get("port", 5432)

            if node_info["name"] in mtree_task.fields.node_list:
                params, conn = mtree_task.connection_pool.get_cluster_node_connection(
                    node_info,
                    client_role=(
                        mtree_task.client_role
                        if mtree_task.invoke_method == "api"
                        else None
                    ),
                )
            else:
                continue

            curr_cols = get_cols(conn, l_schema, l_table)
            curr_key = get_key(conn, l_schema, l_table)

            if not curr_cols:
                raise AceException(
                    f"Table '{mtree_task._table_name}' not found on {hostname}"
                    ", or the current user does not have adequate privileges"
                )
            if not curr_key:
                raise AceException(
                    f"No primary key found for '{mtree_task._table_name}'"
                )

            if (not cols) and (not key):
                cols = curr_cols
                key = curr_key

            if (curr_cols != cols) or (curr_key != key):
                raise AceException("Table schemas don't match")

            cols = curr_cols
            key = curr_key

            col_types = get_col_types(conn, l_table)
            col_types_key = f"{host_ip}:{port}"

            if not mtree_task.fields.col_types:
                mtree_task.fields.col_types = {}

            mtree_task.fields.col_types[col_types_key] = col_types

            authorised, missing_privileges = check_user_privileges(
                conn,
                user,
                l_schema,
                l_table,
                required_privileges,
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
                f'User "{user}" does not have the necessary privileges'
                f" to run {', '.join(missing_privs)} "
                f'on table "{l_schema}.{l_table}" '
                f'on node "{hostname}"'
            )

            if not authorised:
                raise AceException(exception_msg)

            conn_list.append(conn)
            conn_params.append(params)
            host_map[host_ip + ":" + str(port)] = hostname

    except Exception as e:
        raise e

    util.message(
        "Connections successful to nodes in cluster",
        p_state="success",
        quiet_mode=mtree_task.quiet_mode,
    )

    # Psycopg connection objects cannot be pickled easily,
    # so, we send the connection parameters instead
    mtree_task.fields.conn_params = conn_params
    mtree_task.fields.host_map = host_map
    mtree_task.fields.cols = cols
    mtree_task.fields.key = key


def check_repair_option_compatibility(tr_task: TableRepairTask) -> None:
    """
    Checks if the repair options specified are compatible.
    """

    if tr_task.bidirectional:
        if tr_task.upsert_only:
            raise AceException("bidirectional and upsert_only cannot be used together")
        if tr_task.fix_nulls:
            raise AceException("bidirectional and fix_nulls cannot be used together")

    if tr_task.fix_nulls:
        if tr_task.insert_only:
            raise AceException("insert_only and fix_nulls cannot be used together")
        if tr_task.upsert_only:
            raise AceException("upsert_only and fix_nulls cannot be used together")

    if tr_task.upsert_only:
        if tr_task.insert_only:
            raise AceException("insert_only and upsert_only cannot be used together")


def check_if_sot_is_needed(tr_task: TableRepairTask) -> bool:
    """
    Checks if the source of truth is needed for the table repair task.
    """

    if tr_task.fix_nulls:
        return False
    elif tr_task.bidirectional and tr_task.insert_only:
        return False

    return True


def validate_table_repair_inputs(tr_task: TableRepairTask) -> None:
    """
    Validates the basic inputs for a table repair task without establishing connections.
    Raises AceException if validation fails.
    """
    if not tr_task.cluster_name:
        raise AceException("cluster_name is a required argument")

    if not tr_task.diff_file_path:
        raise AceException("diff_file is a required argument")

    tr_task.fix_nulls = parse_bool_field("fix_nulls", tr_task.fix_nulls)
    tr_task.fire_triggers = parse_bool_field("fire_triggers", tr_task.fire_triggers)
    tr_task.dry_run = parse_bool_field("dry_run", tr_task.dry_run)
    tr_task.bidirectional = parse_bool_field("bidirectional", tr_task.bidirectional)
    tr_task.insert_only = parse_bool_field("insert_only", tr_task.insert_only)
    tr_task.upsert_only = parse_bool_field("upsert_only", tr_task.upsert_only)

    # Using a helper function here to avoid clutter
    check_repair_option_compatibility(tr_task)

    sot_needed = check_if_sot_is_needed(tr_task)
    if sot_needed:
        if not tr_task.source_of_truth:
            raise AceException("source_of_truth is a required argument")

    # Check if diff_file exists on disk
    if not os.path.exists(tr_task.diff_file_path):
        raise AceException(f"Diff file {tr_task.diff_file_path} does not exist")

    if tr_task.bidirectional and not tr_task.insert_only:
        raise AceException(
            "insert_only must be True (1) when bidirectional is True (1)"
        )

    found = check_cluster_exists(tr_task.cluster_name)
    if found:
        util.message(
            f"Cluster {tr_task.cluster_name} exists",
            p_state="success",
            quiet_mode=tr_task.quiet_mode,
        )
    else:
        raise AceException(f"Cluster {tr_task.cluster_name} not found")

    nm_lst = tr_task._table_name.split(".")
    if len(nm_lst) != 2:
        raise AceException(
            f"TableName {tr_task._table_name} must be of form" "'schema.table_name'"
        )

    l_schema = nm_lst[0]
    l_table = nm_lst[1]

    db, pg, node_info = cluster.load_json(tr_task.cluster_name)

    cluster_nodes = []
    database = {}

    if tr_task._dbname:
        for db_entry in db:
            if db_entry["db_name"] == tr_task._dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{tr_task._dbname}' "
            + f"not found in cluster '{tr_task.cluster_name}'"
        )

    # TODO: Diff file should be read just once. If MAX_DIFF_ROWS is
    # set to a larger value, reading it once here and then again
    # in the table-diff core is inefficient.
    diff_json = json.load(open(tr_task.diff_file_path, "r"))

    if not diff_json:
        raise AceException("Diff file is empty")

    tr_task.fields.node_list = set(
        chain.from_iterable(key.split("/") for key in diff_json["diffs"].keys())
    )

    # Combine db and cluster_nodes into a single json
    for node in node_info:
        if node["name"] not in tr_task.fields.node_list:
            continue
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    # Check to see if source_of_truth node is present in cluster
    if tr_task.source_of_truth and not tr_task.fix_nulls:
        # Separating out the conditions to make it easier on the eyes
        if not any(
            node.get("name") == tr_task.source_of_truth for node in cluster_nodes
        ):
            raise AceException(
                f"Source of truth node {tr_task.source_of_truth} not present in cluster"
            )

    # Store basic task information
    tr_task.fields.l_schema = l_schema
    tr_task.fields.l_table = l_table
    tr_task.fields.database = database
    tr_task.fields.cluster_nodes = cluster_nodes


def table_repair_checks(
    tr_task: TableRepairTask, skip_validation: bool = False
) -> TableRepairTask:
    """
    Validates and prepares a table repair task.

    This function performs full validation including database connections
    and privilege checks. It should be called in the worker process.

    Returns:
        The validated and prepared task
    """
    if not skip_validation:
        validate_table_repair_inputs(tr_task)

    # Now do connection-specific validation
    cols = None
    key = None
    conn_params = []
    host_map = {}
    required_privileges = ["SELECT", "INSERT", "UPDATE"]

    if not tr_task.upsert_only:
        required_privileges.append("DELETE")

    try:
        for nd in tr_task.fields.cluster_nodes:
            hostname = nd["name"]
            host_ip = nd["public_ip"]
            user = nd["db_user"]
            port = nd.get("port", 5432)

            params, conn = tr_task.connection_pool.get_cluster_node_connection(
                nd,
                client_role=(tr_task.client_role if config.USE_CERT_AUTH else None),
            )

            curr_cols = get_cols(conn, tr_task.fields.l_schema, tr_task.fields.l_table)
            curr_key = get_key(conn, tr_task.fields.l_schema, tr_task.fields.l_table)

            if not curr_cols:
                raise AceException(
                    f"Table '{tr_task._table_name}' not found on {hostname}"
                )
            if not curr_key:
                raise AceException(f"No primary key found for '{tr_task._table_name}'")

            if (not cols) and (not key):
                cols = curr_cols
                key = curr_key

            if (curr_cols != cols) or (curr_key != key):
                raise AceException("Table schemas don't match")

            cols = curr_cols
            key = curr_key

            col_types = get_col_types(conn, tr_task.fields.l_table)
            col_types_key = f"{host_ip}:{str(port)}"

            if not tr_task.fields.col_types:
                tr_task.fields.col_types = {}

            tr_task.fields.col_types[col_types_key] = col_types

            authorised, missing_privileges = check_user_privileges(
                conn,
                user,
                tr_task.fields.l_schema,
                tr_task.fields.l_table,
                required_privileges,
            )

            missing_privs = [
                m.split("_")[1].upper()
                for m in missing_privileges
                if m.split("_")[1].upper() in required_privileges
            ]
            exception_msg = (
                f'User "{user}" does not have the necessary privileges'
                f" to run {', '.join(missing_privs)} "
                f'on table "{tr_task.fields.l_schema}.{tr_task.fields.l_table}" '
                f'on node "{hostname}"'
            )

            if not authorised:
                raise AceException(exception_msg)

            conn_params.append(params)
            host_map[host_ip + ":" + str(port)] = hostname

    except Exception as e:
        raise e

    util.message(
        "Connections successful to nodes in cluster",
        p_state="success",
        quiet_mode=tr_task.quiet_mode,
    )

    tr_task.fields.conn_params = conn_params
    tr_task.fields.host_map = host_map
    tr_task.fields.cols = cols
    tr_task.fields.key = key

    return tr_task


def validate_repset_diff_inputs(rd_task: RepsetDiffTask) -> None:
    """
    Validates the basic inputs for a repset diff task without establishing connections.
    Raises AceException if validation fails.
    """
    if type(rd_task.block_rows) is str:
        try:
            rd_task.block_rows = int(rd_task.block_rows)
        except Exception:
            raise AceException("Invalid values for ACE_BLOCK_ROWS or --block_rows")
    elif type(rd_task.block_rows) is not int:
        raise AceException("Invalid value type for ACE_BLOCK_ROWS or --block_rows")

    # Capping max block size here to prevent the hash function from taking forever
    if rd_task.block_rows > config.MAX_DIFF_BLOCK_SIZE:
        raise AceException(f"Block row size should be <= {config.MAX_DIFF_BLOCK_SIZE}")
    if rd_task.block_rows < config.MIN_DIFF_BLOCK_SIZE:
        raise AceException(f"Block row size should be >= {config.MIN_DIFF_BLOCK_SIZE}")

    if type(rd_task.max_cpu_ratio) is int:
        rd_task.max_cpu_ratio = float(rd_task.max_cpu_ratio)
    elif type(rd_task.max_cpu_ratio) is str:
        try:
            rd_task.max_cpu_ratio = float(rd_task.max_cpu_ratio)
        except Exception:
            raise AceException(
                "Invalid values for ACE_MAX_CPU_RATIO or" "--max_cpu_ratio"
            )
    elif type(rd_task.max_cpu_ratio) is not float:
        raise AceException(
            "Invalid value type for ACE_MAX_CPU_RATIO or" "--max_cpu_ratio"
        )

    if rd_task.max_cpu_ratio > 1.0 or rd_task.max_cpu_ratio < 0.0:
        raise AceException(
            "Invalid value range for ACE_MAX_CPU_RATIO or --max_cpu_ratio"
        )

    if rd_task.output not in ["csv", "json"]:
        raise AceException(
            "Diff-tables currently supports only csv and json output formats"
        )

    node_list = []
    try:
        node_list = parse_nodes(rd_task._nodes)
    except ValueError as e:
        raise AceException(
            "Nodes should be a comma-separated list of nodenames "
            + f'\n\tE.g., --nodes="n1,n2". Error: {e}'
        )

    if len(node_list) > 3:
        raise AceException(
            "diff-tables currently supports up to a three-way table comparison"
        )

    if rd_task._nodes != "all" and len(node_list) == 1:
        raise AceException("repset-diff needs at least two nodes to compare")

    found = check_cluster_exists(rd_task.cluster_name)
    if found:
        util.message(
            f"Cluster {rd_task.cluster_name} exists",
            p_state="success",
            quiet_mode=rd_task.quiet_mode,
        )
    else:
        raise AceException(f"Cluster {rd_task.cluster_name} not found")

    db, pg, node_info = cluster.load_json(rd_task.cluster_name)

    cluster_nodes = []
    database = {}

    if rd_task._dbname:
        for db_entry in db:
            if db_entry["db_name"] == rd_task._dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{rd_task._dbname}' "
            + f"not found in cluster '{rd_task.cluster_name}'"
        )

    for node in node_info:
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    if rd_task._nodes != "all" and len(node_list) > 1:
        for n in node_list:
            if not any(filter(lambda x: x["name"] == n, cluster_nodes)):
                raise AceException("Specified nodenames not present in cluster")

    rd_task.fields.cluster_nodes = cluster_nodes
    rd_task.fields.database = database
    rd_task.fields.node_list = node_list
    rd_task.skip_tables = parse_skip_list(rd_task.skip_tables, rd_task.skip_file)


def repset_diff_checks(
    rd_task: RepsetDiffTask, skip_validation: bool = False
) -> RepsetDiffTask:
    """
    Validates and prepares a repset diff task.

    This function performs full validation including database connections
    and privilege checks. It should be called in the worker process.

    Returns:
        The validated and prepared task
    """
    if not skip_validation:
        validate_repset_diff_inputs(rd_task)

    # Now do connection-specific validation
    conn_list = []

    try:
        for nd in rd_task.fields.cluster_nodes:
            if rd_task._nodes == "all":
                rd_task.fields.node_list.append(nd["name"])

            if (
                rd_task.fields.node_list and nd["name"] in rd_task.fields.node_list
            ) or (not rd_task.fields.node_list):
                _, conn = rd_task.connection_pool.get_cluster_node_connection(
                    nd,
                    client_role=(rd_task.client_role if config.USE_CERT_AUTH else None),
                )
                conn_list.append(conn)

    except Exception as e:
        raise AceException("Error in diff_tbls() Getting Connections:" + str(e), 1)

    util.message(
        "Connections successful to nodes in cluster",
        p_state="success",
        quiet_mode=rd_task.quiet_mode,
    )

    # Connecting to any one of the nodes in the cluster should suffice
    conn = conn_list[0]
    cur = conn.cursor()

    # Check if repset exists
    sql = "select set_name from spock.replication_set;"
    cur.execute(sql)
    repset_list = [item[0] for item in cur.fetchall()]
    if rd_task.repset_name not in repset_list:
        raise AceException(f"Repset {rd_task.repset_name} not found")

    # No need to sanitise repset_name here since psycopg does it for us
    sql = (
        "SELECT concat_ws('.', nspname, relname) FROM spock.tables where set_name = %s;"
    )
    cur.execute(sql, (rd_task.repset_name,))
    tables = cur.fetchall()

    if not tables:
        util.message(
            "Repset may be empty",
            p_state="warning",
            quiet_mode=rd_task.quiet_mode,
        )

    # Convert fetched rows into a list of strings
    rd_task.table_list = [
        table[0] for table in tables if table[0] not in rd_task.skip_tables
    ]

    return rd_task


def validate_spock_diff_inputs(sd_task: SpockDiffTask) -> None:
    """
    Validates the basic inputs for a spock diff task without establishing connections.
    Raises AceException if validation fails.
    """
    node_list = []
    try:
        node_list = parse_nodes(sd_task._nodes)
    except ValueError as e:
        raise AceException(
            "Nodes should be a comma-separated list of nodenames "
            + f'\n\tE.g., --nodes="n1,n2". Error: {e}'
        )

    if sd_task._nodes != "all" and len(node_list) == 1:
        raise AceException("spock-diff needs at least two nodes to compare")

    if len(node_list) > 3:
        raise AceException(
            "spock-diff currently supports up to a three-way table comparison"
        )

    found = check_cluster_exists(sd_task.cluster_name)
    if found:
        util.message(f"Cluster {sd_task.cluster_name} exists", p_state="success")
    else:
        raise AceException(f"Cluster {sd_task.cluster_name} does not exist")

    db, pg, node_info = cluster.load_json(sd_task.cluster_name)

    cluster_nodes = []
    database = {}

    if sd_task._dbname:
        for db_entry in db:
            if db_entry["db_name"] == sd_task._dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{sd_task._dbname}' "
            + f"not found in cluster '{sd_task.cluster_name}'"
        )

    # Combine db and cluster_nodes into a single json
    for node in node_info:
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    if sd_task._nodes != "all" and len(node_list) > 1:
        for n in node_list:
            if not any(filter(lambda x: x["name"] == n, cluster_nodes)):
                raise AceException("Specified nodenames not present in cluster")

    # Store basic task information
    sd_task.fields.cluster_nodes = cluster_nodes
    sd_task.fields.database = database
    sd_task.fields.node_list = node_list


def spock_diff_checks(
    sd_task: SpockDiffTask, skip_validation: bool = False
) -> SpockDiffTask:
    """
    Validates and prepares a spock diff task.

    This function performs full validation including database connections.
    It should be called in the worker process.

    Returns:
        The validated and prepared task
    """
    # First do basic validation
    if not skip_validation:
        validate_spock_diff_inputs(sd_task)

    # Now do connection-specific validation
    conn_params = []
    host_map = {}

    try:
        for nd in sd_task.fields.cluster_nodes:
            if sd_task._nodes == "all":
                sd_task.fields.node_list.append(nd["name"])

            if (
                sd_task.fields.node_list and nd["name"] in sd_task.fields.node_list
            ) or (not sd_task.fields.node_list):
                params, conn = sd_task.connection_pool.get_cluster_node_connection(
                    nd,
                    client_role=(sd_task.client_role if config.USE_CERT_AUTH else None),
                )
                conn_params.append(params)
                host_map[nd["public_ip"] + ":" + str(params["port"])] = nd["name"]

    except Exception as e:
        raise AceException("Error in spock_diff() Getting Connections:" + str(e), 1)

    sd_task.fields.conn_params = conn_params
    sd_task.fields.host_map = host_map

    return sd_task


def validate_schema_diff_inputs(sc_task: SchemaDiffTask) -> SchemaDiffTask:

    node_list = []
    try:
        node_list = parse_nodes(sc_task._nodes)
    except ValueError as e:
        raise AceException(
            "Nodes should be a comma-separated list of nodenames "
            + f'\n\tE.g., --nodes="n1,n2". Error: {e}'
        )

    if sc_task._nodes != "all" and len(node_list) == 1:
        raise AceException("schema-diff needs at least two nodes to compare")

    found = check_cluster_exists(sc_task.cluster_name)

    if found:
        util.message(f"Cluster {sc_task.cluster_name} exists", p_state="success")
    else:
        raise AceException(f"Cluster {sc_task.cluster_name} not found")

    db, pg, node_info = cluster.load_json(sc_task.cluster_name)

    cluster_nodes = []

    database = {}

    if sc_task._dbname:
        for db_entry in db:
            if db_entry["db_name"] == sc_task._dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{sc_task._dbname}' "
            + f"not found in cluster '{sc_task.cluster_name}'"
        )

    # Combine db and cluster_nodes into a single json
    for node in node_info:
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    cluster_node_names = [nd["name"] for nd in cluster_nodes]
    if sc_task._nodes == "all":
        for nd in cluster_node_names:
            node_list.append(nd)

    if len(node_list) > 3:
        raise AceException(
            "schema-diff currently supports up to a three-way table comparison"
        )

    for nd in node_list:
        if nd not in cluster_node_names:
            raise AceException(f'Specified nodename "{nd}" not present in cluster', 1)

    sc_task.fields.cluster_nodes = cluster_nodes
    sc_task.fields.database = database
    sc_task.fields.node_list = node_list
    sc_task.skip_tables = parse_skip_list(sc_task.skip_tables, sc_task.skip_file)

    return sc_task


def schema_diff_checks(
    sc_task: SchemaDiffTask, skip_validation: bool = False
) -> SchemaDiffTask:
    """
    Validates and prepares a schema diff task.

    This function performs full validation including database connections
    and privilege checks. It should be called in the worker process.

    Returns:
        The validated and prepared task
    """
    if not skip_validation:
        validate_schema_diff_inputs(sc_task)

    conn_list = []

    try:
        for nd in sc_task.fields.cluster_nodes:
            if sc_task._nodes == "all":
                sc_task.fields.node_list.append(nd["name"])

            if (
                sc_task.fields.node_list and nd["name"] in sc_task.fields.node_list
            ) or (not sc_task.fields.node_list):
                _, conn = sc_task.connection_pool.get_cluster_node_connection(
                    nd,
                    client_role=(sc_task.client_role if config.USE_CERT_AUTH else None),
                )
                conn_list.append(conn)

    except Exception as e:
        raise AceException("Error in schema_diff() Getting Connections:" + str(e), 1)

    util.message(
        "Connections successful to nodes in cluster",
        p_state="success",
        quiet_mode=sc_task.quiet_mode,
    )

    # Connecting to any one of the nodes in the cluster should suffice
    conn = conn_list[0]
    cur = conn.cursor()

    # Check if schema exists
    schema_sql = "SELECT EXISTS (SELECT 1 from pg_namespace where nspname = %s);"
    cur.execute(schema_sql, (sc_task.schema_name,))
    schema_exists = cur.fetchone()[0]
    if not schema_exists:
        raise AceException(f"Schema {sc_task.schema_name} not found")

    # Using the 'base table' type coz we don't need views and temp tables
    table_list_sql = """
        SELECT
            concat_ws('.', table_schema, table_name) as table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = %s
        AND
            table_type = 'BASE TABLE';
    """
    cur.execute(table_list_sql, (sc_task.schema_name,))
    table_list = cur.fetchall()

    if not table_list:
        raise AceException(f"Schema {sc_task.schema_name} is empty")

    sc_task.table_list = [
        table[0] for table in table_list if table[0] not in sc_task.skip_tables
    ]

    return sc_task


def update_spock_exception_checks(
    cluster_name: str, node_name: str, entry: dict, dbname: str = None
) -> None:

    # This module doesn't necessitate the need to create a task object, so we
    # simply create a standalone instance of ConnectionPool here
    conn_pool = ConnectionPool()

    if not cluster_name or not node_name:
        raise AceException("cluster_name and node_name are required fields")

    if not entry:
        raise AceException("entry containing exception details is a required field")

    found = check_cluster_exists(cluster_name)
    if not found:
        raise AceException(f"Cluster {cluster_name} not found")

    db, pg, node_info = cluster.load_json(cluster_name)

    if node_name not in [nd["name"] for nd in node_info]:
        raise AceException(f'Specified nodename "{node_name}" not present in cluster')

    cluster_nodes = []
    combined_json = {}
    database = {}

    if dbname:
        for db_entry in db:
            if db_entry["db_name"] == dbname:
                database = db_entry
                break
    else:
        database = db[0]

    if not database:
        raise AceException(
            f"Database '{dbname}' " + f"not found in cluster '{cluster_name}'"
        )

    # Combine db and cluster_nodes into a single json
    for node in node_info:
        combined_json = {**database, **node}
        cluster_nodes.append(combined_json)

    if not isinstance(entry, dict):
        try:
            entry = json.loads(entry)
        except json.JSONDecodeError:
            raise AceException("entry must be a valid JSON string")

    remote_origin = entry.get("remote_origin", None)
    remote_commit_ts = entry.get("remote_commit_ts", None)
    remote_xid = entry.get("remote_xid", None)
    status = entry.get("status", None)
    resolution_details = entry.get("resolution_details", None)
    command_counter = entry.get("command_counter", None)

    if not remote_origin or not remote_commit_ts or not remote_xid or not status:
        raise AceException(
            "remote_origin, remote_commit_ts, remote_xid, status are required fields"
        )

    if status not in ["PENDING", "RESOLVED", "UNRESOLVED"]:
        raise AceException("status must be one of PENDING, RESOLVED, or UNRESOLVED")

    if command_counter is not None and not isinstance(command_counter, int):
        raise AceException("command_counter must be an integer")

    if not isinstance(resolution_details, dict):
        try:
            resolution_details = json.loads(resolution_details)
        except json.JSONDecodeError:
            raise AceException("resolution_details must be a valid JSON string")

    conn: psycopg.Connection = None
    """
    We will attempt to establish a connection to the specified node with
    autocommit set to False. This is because we want to make sure that
    both the updates to spock.exception_status and spock.exception_status_detail
    are executed together.
    """
    try:
        for node in cluster_nodes:
            if node["name"] == node_name:
                # FIXME: Figure out connection handling here
                _, conn = conn_pool.get_cluster_node_connection(node)
                conn.autocommit = False

    except Exception as e:
        raise AceException(
            "Error in exception_status_checks() Couldn't connect to specified node"
            + str(e)
        )

    return conn


def handle_task_exception(task, task_context):
    task.scheduler.task_status = "FAILED"
    task.scheduler.finished_at = datetime.now()
    task.scheduler.time_taken = util.round_timedelta(
        datetime.now() - task.scheduler.started_at
    ).total_seconds()
    task.scheduler.task_context = task_context

    skip_update = getattr(task, "skip_db_update", False)

    if not skip_update:
        ace_db.update_ace_task(task)


def error_listener(event):
    if event.exception:
        job_id = event.job_id
        print(f"Job ID: {job_id}")
        job = ace_db.get_pickled_task(job_id)
        print(f"Job details: {str(job)}")


if __name__ == "__main__":
    # Configure the root logger
    logging.basicConfig()

    # Create a StreamHandler for stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG if config.DEBUG_MODE else logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream_handler.setFormatter(formatter)

    # Get the apscheduler logger and add the handler
    apscheduler_logger = logging.getLogger("apscheduler")
    apscheduler_logger.setLevel(logging.DEBUG if config.DEBUG_MODE else logging.INFO)
    apscheduler_logger.addHandler(stream_handler)

    ace_db.create_ace_tables()

    fire.Fire(
        {
            "table-diff": ace_cli.table_diff_cli,
            "table-repair": ace_cli.table_repair_cli,
            "table-rerun": ace_cli.table_rerun_cli,
            "repset-diff": ace_cli.repset_diff_cli,
            "schema-diff": ace_cli.schema_diff_cli,
            "spock-diff": ace_cli.spock_diff_cli,
            "spock-exception-update": ace_cli.update_spock_exception_cli,
            "auto-repair": ace_core.auto_repair,
            "start": ace_daemon.start_ace,
            "mtree": ace_cli.merkle_tree_cli,
        }
    )
