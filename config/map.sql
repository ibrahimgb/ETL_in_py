
DROP TABLE IF EXISTS public.category_transformed;
CREATE TABLE public.category_transformed AS
SELECT
    CAST(category_id AS INTEGER) AS category_id,
    INITCAP(TRIM(name)) AS category_name,
    COALESCE(last_update, CURRENT_DATE) AS last_update,
    CURRENT_TIMESTAMP AS transformed_at
FROM
    public.category
WHERE
    name IS NOT NULL;
