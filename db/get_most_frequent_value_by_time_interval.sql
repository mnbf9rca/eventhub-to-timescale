CREATE OR REPLACE FUNCTION get_most_frequent_value_by_time_interval(
    measurement_subject_param text,
    measurement_of_param text,
    start_time_param timestamp with time zone,
    end_time_param timestamp with time zone
)
RETURNS TABLE("time" timestamp with time zone, most_common_value text)
LANGUAGE plpgsql
AS $$
DECLARE
    total_count integer;
    interval_seconds integer;
BEGIN
    -- Get the total number of rows that match the criteria
    EXECUTE 'SELECT COUNT(*)
             FROM conditions
             WHERE (
                measurement_of = $1
                AND measurement_subject = $2
                AND "timestamp" BETWEEN $3 AND $4
             )'
    INTO total_count
    USING measurement_of_param, measurement_subject_param, start_time_param, end_time_param;

    -- Calculate the duration for each interval
    IF total_count > 360 THEN
        interval_seconds := EXTRACT(EPOCH FROM (end_time_param - start_time_param)) / 360;
    ELSE
        interval_seconds := 60;  -- 1 minute in seconds
    END IF;

    -- Main query using the calculated interval
    RETURN QUERY EXECUTE format('
        SELECT
            timestamp with time zone ''epoch'' + floor(EXTRACT(EPOCH FROM "timestamp") / %s) * %s * interval ''1 second'' AS "time",
            mode() WITHIN GROUP (ORDER BY measurement_string) AS most_common_value
        FROM
            conditions
        WHERE (
            measurement_of = $1
            AND measurement_subject = $2
            AND "timestamp" BETWEEN $3 AND $4
        )
        GROUP BY
            "time"
        ORDER BY
            "time"', interval_seconds, interval_seconds)
    USING measurement_of_param, measurement_subject_param, start_time_param, end_time_param;
END;
$$;
