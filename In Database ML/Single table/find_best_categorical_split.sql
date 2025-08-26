CREATE OR REPLACE FUNCTION diss.find_best_categorical_split(
	table_name text,
	target_col text,
	feature_col text,
	where_clause text)
    RETURNS TABLE(feature_name text, category_value integer, variance_reduction double precision, n_categories integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
  sql TEXT;
  wc  TEXT := CASE WHEN where_clause = '' THEN '' ELSE 'WHERE ' || where_clause END;

  -- Map any categorical value to an int consistently (booleans, numerics-as-text, general text)
  cat_expr TEXT := format(
$$CASE
     WHEN %1$I IS NULL THEN NULL
     WHEN lower(%1$I::text) IN ('t','true') THEN 1
     WHEN lower(%1$I::text) IN ('f','false') THEN 0
     WHEN %1$I::text ~ '^-?[0-9]+$' THEN (%1$I::text)::int4
     ELSE hashtext(%1$I::text)
  END$$, feature_col);
BEGIN
  sql := format($fmt$
    SELECT
      %1$L                                      AS feature_name,
      (%2$s)::int4                              AS category_value,
      categorical_split_agg(%3$I::float8, (%2$s)::int4) AS variance_reduction,
      COUNT(DISTINCT (%2$s)::int4)::int         AS n_categories
    FROM %4$I
    %5$s
    GROUP BY (%2$s)
    ORDER BY variance_reduction DESC
    LIMIT 1
  $fmt$,
    feature_col,
    cat_expr,
    target_col,
    table_name,
    wc
  );

  RETURN QUERY EXECUTE sql;
END;
$BODY$;