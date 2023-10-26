CREATE OR REPLACE FUNCTION get_unique_measurement_subjects(
    start_time_param timestamp with time zone,
    end_time_param timestamp with time zone,
    measurement_of_param text
)
RETURNS TABLE(measurement_subject text)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY EXECUTE '
        SELECT DISTINCT measurement_subject
        FROM conditions
        WHERE
            "timestamp" BETWEEN $1 AND $2
            AND measurement_of = $3
        ORDER BY measurement_subject'
    USING start_time_param, end_time_param, measurement_of_param;
END;
$$;