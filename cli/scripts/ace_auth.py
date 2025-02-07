from copy import deepcopy
import datetime
import ssl
from functools import wraps
from flask import request, jsonify
import psycopg
from cryptography.hazmat.backends import default_backend
from cryptography import x509, __version__ as crypto_version

import ace_config as config
from ace_exceptions import CertificateVerificationError, AuthenticationError
import pgpasslib

if config.USE_NAIVE_DATETIME:
    from packaging import version
    # not_valid_before is deprecated from this version on
    # we will use the _utc equivalents unless we're dealing with an older version
    # https://cryptography.io/en/latest/x509/reference/#cryptography.x509.Certificate.not_valid_before_utc
    CRYPTO_VERSION_WITH_UTC = version.parse("42.0.0")
    USE_UTC_SUFFIX = version.parse(crypto_version) >= CRYPTO_VERSION_WITH_UTC
else:
    # If you're using a version of cryptography that doesn't support the timezone
    # aware datetime objects, and USE_NAIVE_DATETIME is still set to False,
    # this will break.
    USE_UTC_SUFFIX = True

USE_UTC_SUFFIX = True if not config.USE_NAIVE_DATETIME else False


# TODO: Get rid of passing around params
class ConnectionPool:
    """Manages database connections to avoid creating duplicate connections."""

    def __init__(self):
        self._connections = {}
        self._params = {
            "dbname": None,
            "user": None,
            "host": None,
            "port": None,
            "options": f"-c statement_timeout={config.STATEMENT_TIMEOUT}",
            "application_name": "ACE",
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
            "connect_timeout": config.CONNECTION_TIMEOUT,
        }

    def get_conn_from_pool(self, host, port, db_user, dbname):
        """Get an existing connection or return None if not found."""
        key = (host, port, db_user, dbname)
        conn = self._connections.get(key)
        params = deepcopy(self._params)

        params["dbname"] = dbname
        params["user"] = db_user
        params["host"] = host
        params["port"] = port

        if conn:
            try:
                if conn.closed:
                    del self._connections[key]
                    return None, None

                with conn.cursor() as cur:
                    conn.rollback()
                    cur.execute("BEGIN")
                    cur.execute("SELECT 1")
                    cur.execute("COMMIT")

                    # Reset role here. We'll handle privilege dropping later
                    cur.execute("RESET ROLE")
                return params, conn
            except Exception:
                if key in self._connections:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    del self._connections[key]
                return None, None
        return None, None

    def add_conn_to_pool(self, host, port, db_user, dbname, conn):
        """Add a new connection to the pool."""
        key = (host, port, db_user, dbname)
        if key in self._connections:
            try:
                self._connections[key].close()
            except Exception:
                pass
        self._connections[key] = conn

    def connect(self, node_info):
        """
        Create a new independent connection to a node.
        This is the central method for creating new PostgreSQL connections.
        Handles both certificate and password authentication.

        Args:
            node_info: Dictionary containing node connection information:
                - db_name: Database name
                - db_user: Database user
                - public_ip: Host IP/name
                - port: Port number
                - db_password: Password (optional)
        Returns:
            psycopg.Connection: A new database connection
        """
        try:
            params = deepcopy(self._params)
            params["dbname"] = node_info["db_name"]
            params["user"] = node_info["db_user"]
            params["host"] = node_info["public_ip"]
            params["port"] = node_info.get("port", 5432)

            if config.USE_CERT_AUTH:
                params.update(
                    {
                        "sslmode": "verify-full",
                        "sslcert": config.SSL_CERT_FILE,
                        "sslkey": config.SSL_KEY_FILE,
                        "sslrootcert": config.SSL_CA_FILE,
                    }
                )
            elif "db_password" in node_info:
                params["password"] = node_info["db_password"]
            else:
                pgpass = pgpasslib.getpass(
                    host=node_info["public_ip"],
                    user=node_info["db_user"],
                    dbname=node_info["db_name"],
                    port=node_info.get("port", 5432),
                )
                if not pgpass:
                    msg = f"No password found for {node_info['public_ip']}"
                    raise AuthenticationError(msg)
                params["password"] = pgpass

            conn = psycopg.connect(**params)

            return params, conn

        except Exception as e:
            raise AuthenticationError(
                f"Failed to connect to node {node_info['public_ip']}: {str(e)}"
            )

    def get_cluster_node_connection(
        self,
        node_info,
        client_role=None,
        drop_privileges=True,
    ):
        """
        Get a connection to a cluster node, using the connection pool if possible.
        If no pooled connection exists, creates a new one using get_new_conn.

        Args:
            node_info: Dictionary containing node connection information
            cluster_name: Name of the cluster (for error messages)
            invoke_method: Either "cli" or "api" to determine auth method
            client_role: Role to switch to after connection (used in API mode)
            drop_privileges: Whether to drop to client_role after connecting

        Returns:
            tuple: (connection_params, connection)
        """
        host = node_info["public_ip"]
        port = node_info.get("port", 5432)
        db_user = node_info["db_user"]
        dbname = node_info["db_name"]

        params, conn = self.get_conn_from_pool(host, port, db_user, dbname)
        if conn:
            if drop_privileges:
                self.drop_privileges(conn, client_role)
            return params, conn

        params, conn = self.connect(node_info)
        self.add_conn_to_pool(host, port, db_user, dbname, conn)
        return params, conn

    def drop_privileges(self, conn, client_role):
        if config.USE_CERT_AUTH and client_role:
            with conn.cursor() as cur:
                cur.execute(f"SET ROLE {client_role}")

    def close_all(self):
        """Close all connections in the pool."""
        for conn in self._connections.values():
            try:
                conn.close()
            except Exception:
                pass
        self._connections.clear()


def extract_common_name(cert_data):
    """
    Extract the Common Name from a certificate.

    Args:
        cert_data: The certificate data in PEM or DER format

    Returns:
        str: The Common Name from the certificate

    Raises:
        CertificateVerificationError: If CN cannot be extracted
    """
    try:
        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        for attr in cert.subject:
            if attr.oid == x509.NameOID.COMMON_NAME:
                return attr.value
        raise CertificateVerificationError("No Common Name found in certificate")
    except Exception as e:
        msg = f"Failed to extract CN from certificate: {str(e)}"
        raise CertificateVerificationError(msg)


def verify_client_cert(cert_data, ca_cert_path=None):
    """
    Verify a client certificate against the CA certificate.

    Args:
        cert_data: The client certificate data
        ca_cert_path: Path to the CA certificate file. If None, uses
            config.CA_CERT_FILE

    Returns:
        bool: True if verification succeeds

    Raises:
        CertificateVerificationError: If verification fails
    """
    try:
        if ca_cert_path is None:
            ca_cert_path = config.CA_CERT_FILE

        context = ssl.create_default_context(cafile=ca_cert_path)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = False

        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
        if not cert:
            raise CertificateVerificationError("Invalid certificate format")

        not_valid_before = (
            cert.not_valid_before_utc if USE_UTC_SUFFIX else cert.not_valid_before
        )
        not_valid_after = (
            cert.not_valid_after_utc if USE_UTC_SUFFIX else cert.not_valid_after
        )

        if not_valid_after <= not_valid_before:
            raise CertificateVerificationError(
                "Certificate has invalid validity period"
            )

        current_time = datetime.datetime.now(datetime.timezone.utc)
        if current_time < not_valid_before or current_time > not_valid_after:
            raise CertificateVerificationError("Certificate is not currently valid")

        return True
    except CertificateVerificationError:
        raise
    except Exception as e:
        raise CertificateVerificationError(f"Certificate verification failed: {str(e)}")


def require_client_cert(f):
    """
    Decorator to require and verify client certificates for API endpoints.

    Usage:
        @app.route('/some-endpoint')
        @require_client_cert
        def protected_endpoint():
            ...
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            if "SSL_CLIENT_CERT" not in request.environ:
                return jsonify({"error": "Client certificate required"}), 401

            cert_data = request.environ["SSL_CLIENT_CERT"].encode("utf-8")

            verify_client_cert(cert_data)

            client_cn = extract_common_name(cert_data)

            request.client_cn = client_cn

            return f(*args, **kwargs)
        except CertificateVerificationError as e:
            return jsonify({"error": str(e)}), 401
        except Exception as e:
            return jsonify({"error": f"Authentication failed: {str(e)}"}), 500

    return decorated_function
