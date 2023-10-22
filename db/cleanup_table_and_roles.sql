-- pass as table_name parameter e.g.
-- psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB -f db/cleanup_table_and_roles.sql -v table_name='your_table_name'

DO $$
DECLARE
    target_table_name text := :table_name;
    reader_role_name text := target_table_name || '_reader';
    writer_role_name text := target_table_name || '_writer';
    reader_user_name text := target_table_name || '_reader_user';
    writer_user_name text := target_table_name || '_writer_user';
    unique_id_field_name text := 'measurement_unique_id';
    sequence_name text := target_table_name || '_' || unique_id_field_name || '_sequence';
BEGIN
    -- Revoke privileges on the sequence if it exists
    IF EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = sequence_name) THEN
        EXECUTE 'REVOKE USAGE, SELECT ON SEQUENCE ' || sequence_name || ' FROM ' || writer_role_name;
    END IF;

    -- Drop the table if it exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = target_table_name) THEN
        EXECUTE 'DROP TABLE IF EXISTS ' || target_table_name || ' CASCADE';
    END IF;

    -- Drop the sequence if it exists
    IF EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = sequence_name) THEN
        EXECUTE 'DROP SEQUENCE IF EXISTS ' || sequence_name;
    END IF;

    -- Drop the reader_user if it exists
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader_user_name) THEN
        EXECUTE 'DROP ROLE IF EXISTS ' || reader_user_name;
    END IF;

    -- Drop the writer_user if it exists
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = writer_user_name) THEN
        EXECUTE 'DROP ROLE IF EXISTS ' || writer_user_name;
    END IF;

    -- Drop the reader role if it exists
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader_role_name) THEN
        EXECUTE 'DROP ROLE IF EXISTS ' || reader_role_name;
    END IF;

    -- Drop the writer role if it exists
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = writer_role_name) THEN
        EXECUTE 'DROP ROLE IF EXISTS ' || writer_role_name;
    END IF;
END;
$$;
