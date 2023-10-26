CREATE OR REPLACE FUNCTION get_aggregated_data_by_day(
    measurement_subject_param text,
    measurement_of_param text,
    start_time_param timestamp with time zone,
    end_time_param timestamp with time zone
)
RETURNS TABLE("time" timestamp with time zone, avg_measurement_number double precision)
LANGUAGE plpgsql
AS $$
DECLARE
    time_duration interval;
    time_interval interval;
BEGIN
    -- Calculate the time duration between start and end time
    SELECT end_time_param - start_time_param INTO time_duration;

    -- If the duration is less than or equal to 1 day
    IF time_duration <= INTERVAL '1 day' THEN
        -- Calculate the time bucket size for up to 360 buckets
        SELECT (end_time_param - start_time_param) / LEAST(360, EXTRACT(EPOCH FROM time_duration)/900) INTO time_interval;
        
        -- Execute and return the query for average values
        RETURN QUERY EXECUTE '
            SELECT
                date_trunc(''day'', "timestamp") + (INTERVAL ''1 second'' * (floor(EXTRACT(epoch from ("timestamp" - date_trunc(''day'', "timestamp"))) / EXTRACT(epoch FROM $1) ) * EXTRACT(epoch FROM $1))) AS "time",
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

    -- If the duration is more than 1 day
    ELSE
        -- Execute and return the query for max values per day
        RETURN QUERY EXECUTE '
            SELECT
                date_trunc(''day'', "timestamp") AS "time",
                MAX(measurement_number) AS avg_measurement_number
            FROM
                conditions
            WHERE
                (
                    measurement_of = $1
                    AND measurement_subject = $2
                    AND "timestamp" BETWEEN $3 AND $4
                )
            GROUP BY
                "time"
            ORDER BY
                "time"'
        USING measurement_of_param, measurement_subject_param, start_time_param, end_time_param;
    END IF;
END;
$$;
