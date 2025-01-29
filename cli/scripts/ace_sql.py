# Various SQL statements for Merkle Tree operations

CREATE_METADATA_TABLE = """
    CREATE TABLE IF NOT EXISTS ace_mtree_metadata (
        schema_name text,
        table_name text,
        total_rows bigint,
        num_blocks int,
        last_updated timestamptz,
        PRIMARY KEY (schema_name, table_name)
    );
"""

CREATE_MTREE_TABLE = """
    CREATE TABLE ace_mtree_{schema}_{table} (
        node_level integer NOT NULL,
        node_position bigint NOT NULL,
        range_start {pkey_type},
        range_end {pkey_type},
        leaf_hash bytea,
        node_hash bytea,
        dirty boolean DEFAULT false,
        last_modified timestamptz DEFAULT current_timestamp,
        PRIMARY KEY (node_level, node_position)
    );
    CREATE INDEX IF NOT EXISTS ace_mtree_{schema}_{table}_range_idx
    ON ace_mtree_{schema}_{table} (range_start, range_end)
    WHERE node_level = 0;
"""

CREATE_BLOCK_ID_FUNCTION = """
    CREATE OR REPLACE FUNCTION get_block_id_{schema}_{table}({pkey_name} {pkey_type})
    RETURNS bigint AS $$
    DECLARE
        pos bigint;
    BEGIN
        SELECT node_position INTO pos
        FROM ace_mtree_{schema}_{table}
        WHERE node_level = 0
        AND range_start <= {pkey_name}
        AND (range_end > {pkey_name} OR range_end IS NULL)
        ORDER BY node_position
        LIMIT 1;

        IF pos IS NULL THEN
            SELECT node_position INTO pos
            FROM ace_mtree_{schema}_{table}
            WHERE node_level = 0
            ORDER BY node_position
            LIMIT 1;
        END IF;

        RETURN pos;
    END;
    $$ LANGUAGE plpgsql STABLE;
"""

CREATE_TRIGGER_FUNCTION = """
    CREATE OR REPLACE FUNCTION track_dirty_blocks_{schema}_{table}()
    RETURNS trigger AS $$
    DECLARE
        affected_pos bigint;
    BEGIN
        IF TG_OP = 'INSERT' THEN
            affected_pos := get_block_id_{schema}_{table}(NEW.{pkey});
        ELSIF TG_OP = 'DELETE' THEN
            affected_pos := get_block_id_{schema}_{table}(OLD.{pkey});
        ELSIF TG_OP = 'UPDATE' THEN
            IF get_block_id_{schema}_{table}(OLD.{pkey}) !=
            get_block_id_{schema}_{table}(NEW.{pkey}) THEN
                UPDATE ace_mtree_{schema}_{table}
                SET dirty = true,
                    last_modified = current_timestamp
                WHERE node_position IN (
                    get_block_id_{schema}_{table}(OLD.{pkey}),
                    get_block_id_{schema}_{table}(NEW.{pkey})
                )
                AND node_level = 0;
                RETURN NULL;
            END IF;
            affected_pos := get_block_id_{schema}_{table}(NEW.{pkey});
        END IF;

        UPDATE ace_mtree_{schema}_{table}
        SET dirty = true,
            last_modified = current_timestamp
        WHERE node_position = affected_pos
        AND node_level = 0;

        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
"""

CREATE_TRIGGER = """
    DROP TRIGGER IF EXISTS
    track_dirty_blocks_{schema}_{table}_trigger ON {schema}.{table};
    CREATE TRIGGER track_dirty_blocks_{schema}_{table}_trigger
    AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table}
    FOR EACH STATEMENT EXECUTE FUNCTION track_dirty_blocks_{schema}_{table}();
"""

CREATE_XOR_FUNCTION = """
    CREATE OR REPLACE FUNCTION bytea_xor(a bytea, b bytea) RETURNS bytea AS $$
    DECLARE
        result bytea;
        len int;
    BEGIN
        IF length(a) != length(b) THEN
            RAISE EXCEPTION 'bytea_xor inputs must be same length';
        END IF;

        len := length(a);
        result := a;
        FOR i IN 0..len-1 LOOP
            result := set_byte(
                result, i, get_byte(a, i) # get_byte(b, i)
            );
        END LOOP;

        RETURN result;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE STRICT;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_operator WHERE oprname = '#'
            AND oprleft = 'bytea'::regtype AND oprright = 'bytea'::regtype
        ) THEN
            CREATE OPERATOR # (
                LEFTARG = bytea,
                RIGHTARG = bytea,
                PROCEDURE = bytea_xor
            );
        END IF;
    END $$;
"""

ESTIMATE_ROW_COUNT = """
    SELECT (
        CASE
            WHEN s.n_live_tup > 0 THEN s.n_live_tup
            WHEN c.reltuples > 0 THEN c.reltuples
            ELSE pg_relation_size(c.oid) / (8192*0.7)
        END
    )::bigint as estimate
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s
        ON s.schemaname = n.nspname
        AND s.relname = c.relname
    WHERE n.nspname = %s
    AND c.relname = %s
"""

GET_PKEY_TYPE = """
    SELECT a.atttypid::regtype::text
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s
    AND c.relname = %s
    AND a.attname = %s;
"""

UPDATE_METADATA = """
    INSERT INTO ace_mtree_metadata
        (schema_name, table_name, total_rows, num_blocks, last_updated)
    VALUES (%s, %s, %s, %s, current_timestamp)
    ON CONFLICT (schema_name, table_name) DO UPDATE
    SET total_rows = EXCLUDED.total_rows,
        num_blocks = EXCLUDED.num_blocks,
        last_updated = EXCLUDED.last_updated;
"""

CALCULATE_BLOCK_RANGES = """
    WITH sampled_data AS (
        SELECT
            {key}
        FROM {schema}.{table}
        TABLESAMPLE SYSTEM(1)
        ORDER BY {key}
    ),
    first_row AS (
        SELECT
            {key}
        FROM {schema}.{table}
        ORDER BY {key}
        LIMIT 1
    ),
    last_row AS (
        SELECT
            {key}
        FROM {schema}.{table}
        ORDER BY {key} DESC
        LIMIT 1
    ),
    sample_boundaries AS (
        SELECT
            {key},
            ntile({num_blocks}) OVER (ORDER BY {key}) as bucket
        FROM sampled_data
    ),
    block_starts AS (
        SELECT DISTINCT ON (bucket)
            {key}
        FROM sample_boundaries
        ORDER BY bucket, {key}
    ),
    all_bounds AS (
        SELECT
            (SELECT {key} FROM first_row) as {key},
            0 as seq
        UNION ALL
        SELECT
            {key},
            1 as seq
        FROM block_starts
        WHERE {key} > (SELECT {key} FROM first_row)
        UNION ALL
        SELECT
            (SELECT {key} FROM last_row) as {key},
            2 as seq
    ),
    ranges AS (
        SELECT
            {key},
            {key} as range_start,
            LEAD({key}) OVER (ORDER BY seq, {key}) as range_end,
            seq
        FROM all_bounds
    )
    INSERT INTO ace_mtree_{schema}_{table}
        (node_level, node_position, range_start, range_end, last_modified)
    SELECT
        0,
        row_number() OVER (ORDER BY seq, {key}) - 1 as node_position,
        range_start,
        range_end,
        current_timestamp
    FROM ranges
    WHERE range_end IS NOT NULL;
"""

COMPUTE_LEAF_HASHES = """
    WITH block_rows AS (
        SELECT *
        FROM {schema}.{table}
        WHERE {key} >= %(range_start)s
        AND ({key} < %(range_end)s OR %(range_end)s IS NULL)
    ),
    block_hash AS (
        SELECT
            digest(
                COALESCE(
                    string_agg(
                        concat_ws(
                            '|',
                            {columns}
                        ),
                        '|'
                        ORDER BY {key}
                    ),
                    'EMPTY_BLOCK'
                ),
                'sha256'
            ) as leaf_hash
        FROM block_rows
    )
    UPDATE ace_mtree_{schema}_{table} mt
    SET
        leaf_hash = block_hash.leaf_hash,
        node_hash = block_hash.leaf_hash,
        last_modified = current_timestamp
    FROM block_hash
    WHERE mt.node_position = %(node_position)s
    AND mt.node_level = 0
    RETURNING mt.node_position;
"""

GET_BLOCK_RANGES = """
    SELECT node_position, range_start, range_end
    FROM ace_mtree_{schema}_{table}
    WHERE node_level = 0
    ORDER BY node_position;
"""

BUILD_PARENT_NODES = """
    WITH pairs AS (
        SELECT
            node_level,
            node_position / 2 as parent_position,
            array_agg(node_hash ORDER BY node_position) as child_hashes
        FROM ace_mtree_{schema}_{table}
        WHERE node_level = %(node_level)s
        GROUP BY node_level, node_position / 2
    ),
    inserted AS (
        INSERT INTO ace_mtree_{schema}_{table}
            (node_level, node_position, node_hash, last_modified)
        SELECT
            %(node_level)s + 1,
            parent_position,
            CASE
                WHEN array_length(child_hashes, 1) = 1 THEN child_hashes[1]
                ELSE child_hashes[1] # child_hashes[2]
            END,
            current_timestamp
        FROM pairs
        RETURNING 1
    )
    SELECT count(*) FROM inserted;
"""

INSERT_BLOCK_RANGES = """
    INSERT INTO ace_mtree_{schema}_{table}
        (node_level, node_position, range_start, range_end, last_modified)
    VALUES
        (0, %s, %s, %s, current_timestamp);
"""
