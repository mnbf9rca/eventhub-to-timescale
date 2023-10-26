-- Create an instance of the conditions table named after the table_name parameter
-- pass as table_name parameter e.g.
-- psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB -f db/create_table_and_roles.sql -v table_name='your_table_name' --set ON_ERROR_STOP=on
-- setting --set ON_ERROR_STOP=on ensures that psql returns an error code if the script fails
SET session "myapp.table_name" = :table_name;

DO $$
DECLARE
    target_table_name text := current_setting('myapp.table_name');
    reader_role_name text := target_table_name || '_reader';
    writer_role_name text := target_table_name || '_writer';
    reader_user_name text := target_table_name || '_reader_user';
    writer_user_name text := target_table_name || '_writer_user';
    unique_id_field_name text := 'measurement_unique_id';
    sequence_name text := target_table_name || '_' || unique_id_field_name || '_sequence';
BEGIN


    -- install timesdcale
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
    RAISE NOTICE 'Extensions: %', (SELECT * FROM pg_extension);

    -- Create the sequence
    EXECUTE 'CREATE SEQUENCE IF NOT EXISTS ' || sequence_name || ' START 1';

    -- Create the table
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || target_table_name || ' (
        "timestamp"             timestamp with time zone NOT NULL,
        "measurement_subject"   text NOT NULL,
        "measurement_number"    double precision,
        "measurement_of"        text NOT NULL,
        "measurement_string"    text,
        "correlation_id"        text,
        "measurement_bool"        boolean,
        "measurement_publisher" text,
        "measurement_location" geography(Point,4326),
        ' || unique_id_field_name || ' bigint NOT NULL DEFAULT nextval(''' || sequence_name || '''),
        PRIMARY KEY (' || unique_id_field_name || ')
    )';

    -- Create indexes
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_correlation_id_idx ON ' || target_table_name || ' (correlation_id)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_measurement_bool_idx ON ' || target_table_name || ' (measurement_bool)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_measurement_number_idx ON ' || target_table_name || ' (measurement_number)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_measurement_of_idx ON ' || target_table_name || ' USING hash (measurement_of)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_measurement_publisher_idx ON ' || target_table_name || ' (measurement_publisher)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_measurement_string_idx ON ' || target_table_name || ' (measurement_string)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_measurement_subject_idx ON ' || target_table_name || ' (measurement_subject)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || target_table_name || '_timestamp_idx ON ' || target_table_name || ' ("timestamp" DESC)';

    -- convert the table to a hypertable
    PERFORM create_hypertable(target_table_name, 'timestamp');

    -- Check if reader role exists, create if not
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader_role_name) THEN
        EXECUTE 'CREATE ROLE ' || reader_role_name;
    END IF;


    -- Check if writer role exists, create if not
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = writer_role_name) THEN
        EXECUTE 'CREATE ROLE ' || writer_role_name;
    END IF;



    -- Grant SELECT privilege to the reader role
    EXECUTE 'GRANT SELECT ON TABLE ' || target_table_name || ' TO ' || reader_role_name;

    -- Grant INSERT, UPDATE, DELETE, SELECT privileges to the writer role
    EXECUTE 'GRANT INSERT, UPDATE, DELETE, SELECT ON TABLE ' || target_table_name || ' TO ' || writer_role_name;

    -- Assign privileges on the sequence
    EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE ' || sequence_name || ' TO ' || writer_role_name;
    EXECUTE 'GRANT SELECT ON SEQUENCE ' || sequence_name || ' TO ' || reader_role_name;

    -- check if writer user exists, create if not
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = writer_user_name) THEN
        EXECUTE 'CREATE USER ' || writer_user_name || ' WITH PASSWORD ''' || writer_user_name || '''';
    END IF;

    -- add writer user to writer role
    EXECUTE 'GRANT ' || writer_role_name || ' TO ' || writer_user_name;
    EXECUTE 'ALTER USER ' || writer_user_name || ' SET ROLE ' || writer_role_name;

    -- check if reader user exists, create if not
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = reader_user_name) THEN
        EXECUTE 'CREATE USER ' || reader_user_name || ' WITH PASSWORD ''' || reader_user_name || '''';
    END IF;

    -- add reader user to reader role
    EXECUTE 'GRANT ' || reader_role_name || ' TO ' || reader_user_name;
    EXECUTE 'ALTER USER ' || reader_user_name || ' SET ROLE ' || reader_role_name;


END;
$$;
