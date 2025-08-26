CREATE OR REPLACE FUNCTION diss.find_all_numeric_splits_one_pass(
	table_name text,
	target_col text,
	feature_col text,
	where_clause text,
	p_cardinality_cutoff integer DEFAULT 200,
	p_num_quantiles integer DEFAULT 100)
    RETURNS TABLE(threshold double precision, variance_reduction double precision, left_count bigint, right_count bigint) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
  wc               TEXT     := CASE WHEN where_clause = '' THEN '1=1' ELSE where_clause END;
  v_distinct_count BIGINT;
  thresholds_sql   TEXT;
  thresholds_arr   FLOAT8[];
BEGIN
  EXECUTE format(
    'SELECT COUNT(DISTINCT %I) FROM %I WHERE %s',
    feature_col, table_name, wc
  ) INTO v_distinct_count;

  IF v_distinct_count <= p_cardinality_cutoff THEN
    thresholds_sql := format(
      'SELECT DISTINCT %I::float8 AS threshold FROM %I WHERE %s',
      feature_col, table_name, wc
    );
  ELSE
    EXECUTE format($q$
      SELECT
        percentile_disc(
          ARRAY(
            SELECT generate_series(1, %1$s)::double precision/(%1$s+1)
          )
        ) WITHIN GROUP (ORDER BY %2$I)
      FROM %3$I
      WHERE %4$s
    $q$,
      p_num_quantiles,
      feature_col,
      table_name,
      wc
    ) INTO thresholds_arr;

    thresholds_sql := format(
      'SELECT DISTINCT unnest(%L::float8[]) AS threshold',
      thresholds_arr
    );
  END IF;

  RETURN QUERY EXECUTE format($sql$
    WITH thresholds   AS ( %1$s ),
         base_stats   AS (
           SELECT
             th.threshold            AS threshold,
             COUNT(*)                AS cnt,
             SUM(t.%3$I::float8)     AS sum1,
             SUM((t.%3$I::float8)^2) AS sum2
           FROM %2$I AS t
           JOIN thresholds AS th
             ON t.%4$I::float8 = th.threshold
           WHERE %5$s
           GROUP BY th.threshold
         ),
         total_stats  AS (
           SELECT
             SUM(cnt)::bigint            AS tot_cnt,
             SUM(sum1)::double precision AS tot_s1,
             SUM(sum2)::double precision AS tot_s2
           FROM base_stats
         ),
         prefix       AS (
           SELECT
             threshold,
             cnt,
             sum1,
             sum2,
             SUM(cnt)  OVER (ORDER BY threshold) AS left_cnt,
             SUM(sum1) OVER (ORDER BY threshold) AS left_s1,
             SUM(sum2) OVER (ORDER BY threshold) AS left_s2
           FROM base_stats
         )
    SELECT
      p.threshold,
      (t.tot_s2/t.tot_cnt - (t.tot_s1/t.tot_cnt)^2)
      - (p.left_s2/p.left_cnt - (p.left_s1/p.left_cnt)^2) * (p.left_cnt/t.tot_cnt)
      - (((t.tot_s2 - p.left_s2)/(t.tot_cnt - p.left_cnt))
         - ((t.tot_s1 - p.left_s1)/(t.tot_cnt - p.left_cnt))^2)
        * ((t.tot_cnt - p.left_cnt)/t.tot_cnt)
        AS variance_reduction,
      p.left_cnt::bigint               AS left_count,
      (t.tot_cnt - p.left_cnt)::bigint AS right_count
    FROM prefix p
    CROSS JOIN total_stats t
    WHERE p.left_cnt  > 0
      AND p.left_cnt  <  t.tot_cnt
    ORDER BY variance_reduction DESC
    LIMIT 1
  $sql$,
    thresholds_sql,
    table_name,
    target_col,
    feature_col,
    wc
  );
END;
$BODY$;