CREATE OR REPLACE FUNCTION filter_unchanged_rows(
    measurement_subject_param TEXT,
    measurement_of_param TEXT,
    start_time_param TIMESTAMP WITH TIME ZONE,
    end_time_param TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (
    "timestamp" TIMESTAMP WITH TIME ZONE,
    "measurement_subject" TEXT,
    "measurement_of" TEXT,
    "measurement_string" TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c."timestamp",
        c."measurement_subject",
        c."measurement_of",
        c."measurement_string"
    FROM (
        SELECT
            cond.*,
            LAG(cond."measurement_string") OVER (
                PARTITION BY cond."measurement_subject"
                ORDER BY cond."timestamp"
            ) AS prev_measurement_string
        FROM "public"."conditions" AS cond
        WHERE 
            cond."timestamp" BETWEEN start_time_param AND end_time_param AND
            cond."measurement_subject" = measurement_subject_param AND
            cond."measurement_of" = measurement_of_param
    ) AS c
    WHERE c."measurement_string" IS DISTINCT FROM c.prev_measurement_string
    ORDER BY c."timestamp";
END;
$$ LANGUAGE plpgsql;
