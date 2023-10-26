CREATE OR REPLACE FUNCTION get_sampled_data(
    measurement_subject_param text,
    measurement_of_param text,
    start_time_param timestamp,
    end_time_param timestamp,
    resolution_param int
)
RETURNS TABLE("time" timestamp with time zone, smoothed_value double precision)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Execute and return the query using asap_smooth
    RETURN QUERY EXECUTE '
        SELECT 
            (unnest(asap_smooth)).time AS "time",
            (unnest(asap_smooth)).value AS smoothed_value
        FROM (
            SELECT 
                asap_smooth("timestamp", measurement_number, $1) AS asap_smooth
            FROM 
                conditions
            WHERE
                (
                    measurement_of = $2
                    AND measurement_subject = $3
                    AND "timestamp" BETWEEN $4 AND $5
                )
        ) sub_query'
    USING resolution_param, measurement_of_param, measurement_subject_param, start_time_param, end_time_param;
END;
$$;
