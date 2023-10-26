CREATE OR REPLACE FUNCTION get_sampled_data_with_time_weight(
    measurement_subject_param text,
    measurement_of_param text,
    start_time_param timestamp,
    end_time_param timestamp,
    method_param text,
    resolution_param int
)
RETURNS TABLE(dt timestamp with time zone, time_weighted_value double precision)
LANGUAGE plpgsql
AS $$
DECLARE
    time_interval interval;
BEGIN
    -- Calculate the time bucket size to get closest to the desired resolution
    SELECT (end_time_param - start_time_param) / resolution_param INTO time_interval;

    -- Execute and return the query using time_weight
    RETURN QUERY EXECUTE '
        WITH t AS (
            SELECT
                time_bucket($1, "timestamp") AS dt,
                time_weight($6, "timestamp", measurement_number) AS tw
            FROM
                conditions
            WHERE
                (
                    measurement_of = $2
                    AND measurement_subject = $3
                    AND "timestamp" BETWEEN $4 AND $5
                )
            GROUP BY time_bucket($1, "timestamp")
        )
        SELECT
            dt AS "time",
            average(tw) AS time_weighted_value
        FROM t'
    USING time_interval, measurement_of_param, measurement_subject_param, start_time_param, end_time_param, method_param;
END;
$$;
