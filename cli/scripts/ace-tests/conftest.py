# flake8: disable=unused-import

import importlib.util
import os
import sys
from time import sleep
import pytest
import psycopg
import test_config
from test_simple_base import TestSimpleBase
from test_simple import TestSimple

# Set up paths
os.environ["PGEDGE_HOME"] = test_config.PGEDGE_HOME
os.environ["MY_HOME"] = test_config.PGEDGE_HOME
os.environ["MY_LOGS"] = os.path.join(
    test_config.PGEDGE_HOME, "data", "logs", "cli_log.out"
)
os.environ["MY_LITE"] = os.path.join(
    test_config.PGEDGE_HOME, "data", "conf", "db_local.db"
)
os.environ["MY_CMD"] = "pgedge"


def load_mod(mod: str):
    """Loads a module as a callable object"""
    try:
        script_dir = os.path.join("hub", "scripts")
        ace_path = os.path.join(script_dir, f"{mod}.py")
        ace_dir = os.path.dirname(ace_path)
        sys.path.insert(0, ace_dir)

        spec = importlib.util.spec_from_file_location(mod, ace_path)
        ace = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ace)
        sys.path.pop(0)

        return ace

    except Exception as e:
        print(f"Error: loading {mod}: {e}")
        exit(1)


@pytest.fixture(scope="session", autouse=True)
def set_run_dir():
    os.chdir(test_config.PGEDGE_HOME)


@pytest.fixture(scope="session")
def cli():
    return load_mod("ace_cli")


@pytest.fixture(scope="session")
def core():
    return load_mod("ace_core")


@pytest.fixture(scope="session")
def ace_daemon():
    return load_mod("ace_daemon")


@pytest.fixture(scope="session")
def nodes():
    return ["n1", "n2", "n3"]


@pytest.fixture(scope="session")
def ace_conf():
    class Config:
        pass

    ace_config = Config()
    ace_config.CA_CERT_FILE = os.path.join(
        test_config.PGEDGE_HOME, "data", "pg16", "pki", "ca.crt"
    )
    ace_config.ACE_USER_CERT_FILE = os.path.join(
        test_config.PGEDGE_HOME, "data", "pg16", "pki", "ace_user-cert", "ace_user.crt"
    )
    ace_config.ACE_USER_KEY_FILE = os.path.join(
        test_config.PGEDGE_HOME, "data", "pg16", "pki", "ace_user-cert", "ace_user.key"
    )

    ace_config.ADMIN_CERT_FILE = os.path.join(
        test_config.PGEDGE_HOME, "data", "pg16", "pki", "admin-cert", "admin.crt"
    )
    ace_config.ADMIN_KEY_FILE = os.path.join(
        test_config.PGEDGE_HOME, "data", "pg16", "pki", "admin-cert", "admin.key"
    )

    ace_config.USE_CERT_AUTH = True
    return ace_config


@pytest.fixture(scope="class")
def diff_file_path():
    """Fixture to store and share the diff file path between tests"""

    class DiffPathHolder:
        path = None

    return DiffPathHolder


@pytest.fixture(scope="session")
def prepare_databases(nodes):
    """Setup fixture that prepares all databases before running tests"""

    def prepare_node(node):

        # Our table definitions
        customers_sql = """
        CREATE TABLE customers (
            index INTEGER PRIMARY KEY NOT NULL,
            customer_id TEXT NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            company TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            phone_1 TEXT NOT NULL,
            phone_2 TEXT NOT NULL,
            email TEXT NOT NULL,
            subscription_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            website TEXT NOT NULL
        );
        """

        datatypes_sql = """
        CREATE TABLE datatypes_test (
            id UUID PRIMARY KEY,
            int_col INTEGER,
            float_col DOUBLE PRECISION,
            array_col INTEGER[],
            json_col JSONB,
            bytea_col BYTEA,
            point_col POINT,
            text_col TEXT,
            text_array_col TEXT[]
        );
        """

        node_create_sql = f"""
        SELECT spock.node_create('{node}', 'host={node} user=test dbname=demo')
        """
        repset_create_sql = """
        SELECT spock.repset_create('test_repset')
        """

        try:
            params = {
                "host": node if node != "n1" else "localhost",
                "dbname": "demo",
                "user": "admin",
                "application_name": "ace-tests",
            }

            conn = psycopg.connect(**params)
            cur = conn.cursor()

            # Creating the tables first
            cur.execute(customers_sql)
            cur.execute(datatypes_sql)

            cur.execute(
                """
                INSERT INTO datatypes_test VALUES
                (
                    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                    42,
                    3.14159,
                    ARRAY[1, 2, 3, 4, 5],
                    '{"key": "value", "nested": {"foo": "bar"}}',
                    decode('DEADBEEF', 'hex'),
                    point(1.5, 2.5),
                    'sample text',
                    ARRAY['apple', 'banana', 'cherry']
                ),
                (
                    'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12',
                    100,
                    2.71828,
                    ARRAY[10, 20, 30],
                    '{"numbers": [1, 2, 3], "active": true}',
                    decode('BADDCAFE', 'hex'),
                    point(3.7, 4.2),
                    'another sample',
                    ARRAY['dog', 'cat', 'bird']
                ),
                (
                    'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13',
                    -17,
                    0.577216,
                    ARRAY[]::INTEGER[],
                    '{"empty": true}',
                    NULL,
                    point(0, 0),
                    'third sample',
                    ARRAY[]::TEXT[]
                )
            """
            )

            with open(test_config.CUSTOMERS_CSV, "r") as f:
                with cur.copy(
                    "COPY customers FROM STDIN CSV HEADER DELIMITER ','"
                ) as copy:
                    copy.write(f.read())

            cur.execute(node_create_sql)
            cur.execute(repset_create_sql)

            conn.commit()

            print(f"Successfully prepared database on node {node}")

            # Clean up
            cur.close()
            conn.close()
            return True

        except Exception as e:
            print(f"Error executing SQL on node {node}: {str(e)}")
            return False

    for node in nodes:
        prepare_node(node)

    def prepare_spock(node):
        sub_sql = """
        SELECT
        spock.sub_create
        ('sub_{provider_node}{node}', 'host={provider_node} user=test dbname=demo')
        """

        sub_add_sql = """
        SELECT spock.sub_add_repset('sub_{provider_node}{node}', 'test_repset')
        """

        repset_add_customers_sql = """
        SELECT spock.repset_add_table('test_repset', 'customers')
        """

        repset_add_datatypes_sql = """
        SELECT spock.repset_add_table('test_repset', 'datatypes_test')
        """

        try:
            params = {
                "host": node if node != "n1" else "localhost",
                "dbname": "demo",
                "user": "admin",
                "application_name": "ace-tests",
            }

            conn = psycopg.connect(**params)
            cur = conn.cursor()

            for provider_node in nodes:
                if node != provider_node:
                    cur.execute(sub_sql.format(provider_node=provider_node, node=node))
                    cur.execute(
                        sub_add_sql.format(provider_node=provider_node, node=node)
                    )

            cur.execute(repset_add_customers_sql)
            cur.execute(repset_add_datatypes_sql)

            conn.commit()

            print(f"Successfully prepared Spock on node {node}")

            cur.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Error preparing Spock on node {node}: {str(e)}")
            return False

    for node in nodes:
        prepare_spock(node)

    sleep(5)


@pytest.fixture(scope="session", autouse=True)
def cleanup_databases(nodes):
    """Cleanup all databases after running tests"""

    # Yield to let the tests run first
    yield

    # Cleanup code that runs after all tests complete
    drop_customers_sql = "DROP TABLE IF EXISTS customers CASCADE;"
    drop_datatypes_sql = "DROP TABLE IF EXISTS datatypes_test CASCADE;"

    repset_remove_customers = """
    SELECT spock.repset_remove_table('test_repset', 'customers')
    """

    repset_remove_datatypes = """
    SELECT spock.repset_remove_table('test_repset', 'datatypes_test')
    """

    sub_remove_sql = """
    SELECT spock.sub_remove_repset('sub_{provider_node}{node}', 'test_repset')
    """

    sub_drop_sql = """
    SELECT spock.sub_drop('sub_{provider_node}{node}')
    """

    repset_drop_sql = """
    SELECT spock.repset_drop('test_repset')
    """

    node_drop_sql = """
    SELECT spock.node_drop('{node}')
    """

    for node in nodes:
        other_nodes = [n for n in nodes if n != node]
        try:
            params = {
                "host": node if node != "n1" else "localhost",
                "dbname": "demo",
                "user": "admin",
                "application_name": "ace-tests",
            }

            conn = psycopg.connect(**params)
            cur = conn.cursor()

            cur.execute(repset_remove_customers)
            print("remove customers from test_repset", cur.fetchone())
            cur.execute(repset_remove_datatypes)
            print("remove datatypes from test_repset", cur.fetchone())

            for provider_node in other_nodes:
                cur.execute(
                    sub_remove_sql.format(provider_node=provider_node, node=node)
                )
                print(
                    "remove subscription for",
                    provider_node,
                    "on",
                    node,
                    cur.fetchone(),
                )
                cur.execute(sub_drop_sql.format(provider_node=provider_node, node=node))
                print(
                    "drop subscription for",
                    provider_node,
                    "on",
                    node,
                    cur.fetchone(),
                )

            cur.execute(repset_drop_sql)
            print("drop repset", cur.fetchone())

            cur.execute(drop_customers_sql)
            print("drop customers", cur.statusmessage)
            cur.execute(drop_datatypes_sql)
            print("drop datatypes", cur.statusmessage)

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Failed to cleanup node {node}: {str(e)}")

    # We need to drop nodes *after* we've dropped subs and repsets
    for node in nodes:
        try:
            params = {
                "host": node if node != "n1" else "localhost",
                "dbname": "demo",
                "user": "admin",
                "application_name": "ace-tests",
            }

            conn = psycopg.connect(**params)
            cur = conn.cursor()
            cur.execute(node_drop_sql.format(node=node))
            print("drop node", cur.fetchone())
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Failed to cleanup node {node}: {str(e)}")


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "abstract_base: mark a test class as an abstract base class "
        "that should not be run directly",
    )


def pytest_collection_modifyitems(items):
    """Skip tests marked as abstract_base if they are in the base class."""
    for item in items:
        if item.get_closest_marker("abstract_base"):
            # Skip only if the test is in TestSimpleBase class directly
            # or if the test method is not overridden in the child class
            if item.cls and (
                (
                    item.cls.__name__ == "TestSimpleBase"
                    and (
                        issubclass(item.cls, TestSimpleBase)
                        and item.function.__qualname__.startswith("TestSimpleBase.")
                    )
                )
                or (
                    issubclass(item.cls, TestSimple)
                    and item.function.__qualname__.startswith("TestSimple.")
                )
            ):
                item.add_marker(pytest.mark.skip(reason="Abstract base class"))
