CREATE OR REPLACE FUNCTION format_time_intervals(
    measurement_subject_param TEXT,
    measurement_of_param TEXT,
    start_time_param TIMESTAMP WITH TIME ZONE,
    end_time_param TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (
    "time" TIMESTAMP WITH TIME ZONE,
    "timeEnd" TIMESTAMP WITH TIME ZONE,
    "measurement_string" TEXT
) AS $$
DECLARE
    rec RECORD;
    last_measurement_string TEXT := NULL;
    next_time TIMESTAMP WITH TIME ZONE := NULL;
BEGIN
    FOR rec IN (
        SELECT * FROM filter_unchanged_rows(
            measurement_subject_param, 
            measurement_of_param,
            start_time_param, 
            end_time_param
        ) ORDER BY "timestamp" ASC
    ) LOOP
        IF next_time IS NOT NULL THEN
            "time" := next_time;
            "timeEnd" := rec."timestamp";
            "measurement_string" := last_measurement_string;
            RETURN NEXT;
        END IF;
        next_time := rec."timestamp";
        last_measurement_string := rec."measurement_string";
    END LOOP;
    IF next_time IS NOT NULL THEN
        "time" := next_time;
        "timeEnd" := now();
        "measurement_string" := last_measurement_string;
        RETURN NEXT;
    END IF;
END;
$$ LANGUAGE plpgsql;
