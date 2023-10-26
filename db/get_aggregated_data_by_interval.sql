CREATE OR REPLACE FUNCTION get_aggregated_data_by_interval(
    measurement_subject_param text,
    measurement_of_param text,
    start_time_param timestamp,
    end_time_param timestamp,
    time_interval interval
)
RETURNS TABLE("time" timestamp with time zone, avg_measurement_number double precision)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Execute and return the query
    RETURN QUERY EXECUTE '
        SELECT
            time_bucket($1, "timestamp") AS "time",
            AVG(measurement_number) AS avg_measurement_number
        FROM
            conditions
        WHERE
            (
                measurement_of = $2
                AND measurement_subject = $3
                AND "timestamp" BETWEEN $4 AND $5
            )
        GROUP BY
            "time"
        ORDER BY
            "time"'
    USING time_interval, measurement_of_param, measurement_subject_param, start_time_param, end_time_param;
END;
$$;
