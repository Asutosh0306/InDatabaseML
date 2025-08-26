CREATE OR REPLACE FUNCTION diss_joins.find_best_categorical_split_joined(
	feature_col text,
	feature_table text,
	where_clause_sales text DEFAULT '1=1'::text,
	where_clause_stores text DEFAULT '1=1'::text,
	where_clause_items text DEFAULT '1=1'::text)
    RETURNS TABLE(feature_name text, category_value integer, variance_reduction double precision, n_categories integer, left_count bigint, right_count bigint) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    sql TEXT;
    join_sql TEXT;
    cat_expr TEXT;
BEGIN
    -- Build the appropriate join
    join_sql := format('
        FROM diss_joins.train_sales s
        INNER JOIN diss_joins.stores st ON s.store_nbr = st.store_nbr  
        INNER JOIN diss_joins.items i ON s.item_nbr = i.item_nbr
        WHERE (%s) AND (%s) AND (%s)',
        where_clause_sales, where_clause_stores, where_clause_items
    );
    
    -- Map categorical values to integers consistently
    CASE feature_table
        WHEN 'stores' THEN 
            cat_expr := format('COALESCE(hashtext(st.%I::text), 0)', feature_col);
        WHEN 'items' THEN
            cat_expr := format('COALESCE(hashtext(i.%I::text), 0)', feature_col);  
        WHEN 'sales' THEN
            cat_expr := format('COALESCE(hashtext(s.%I::text), 0)', feature_col);
    END CASE;

    sql := format($fmt$
        WITH split_stats AS (
            SELECT 
                %2$s AS category_value,
                COUNT(*) AS cnt,
                SUM(s.unit_sales::float8) AS sum_target,
                SUM((s.unit_sales::float8)^2) AS sum_target_sq
            %3$s
            GROUP BY %2$s
        ),
        category_eval AS (
            SELECT 
                category_value,
                cnt,
                CASE WHEN cnt > 1 
                     THEN (sum_target_sq - (sum_target * sum_target) / cnt) / cnt
                     ELSE 0.0 
                END AS category_variance
            FROM split_stats  
        ),
        total_stats AS (
            SELECT 
                SUM(cnt)::bigint AS total_cnt,
                SUM(sum_target) AS total_sum,
                SUM(sum_target_sq) AS total_sum_sq
            FROM split_stats
        ),
        best_split AS (
            SELECT 
                ce.category_value,
                ce.cnt AS left_cnt,
                (ts.total_cnt - ce.cnt) AS right_cnt,
                -- Calculate variance reduction for this specific category split
                CASE WHEN ts.total_cnt > 1 
                     THEN (ts.total_sum_sq - (ts.total_sum * ts.total_sum) / ts.total_cnt) / ts.total_cnt
                          - (ce.cnt * ce.category_variance + 
                             (ts.total_cnt - ce.cnt) * 
                             CASE WHEN (ts.total_cnt - ce.cnt) > 1
                                  THEN ((ts.total_sum_sq - sum_target_sq) - 
                                        ((ts.total_sum - sum_target) * (ts.total_sum - sum_target)) / (ts.total_cnt - ce.cnt)) / (ts.total_cnt - ce.cnt)
                                  ELSE 0.0 
                             END
                            ) / ts.total_cnt
                     ELSE 0.0 
                END AS variance_reduction
            FROM category_eval ce, total_stats ts, split_stats ss
            WHERE ce.category_value = ss.category_value
            ORDER BY variance_reduction DESC
            LIMIT 1
        )
        SELECT 
            %1$L AS feature_name,
            bs.category_value,
            bs.variance_reduction,
            (SELECT COUNT(DISTINCT category_value) FROM category_eval)::integer AS n_categories,
            bs.left_cnt,
            bs.right_cnt
        FROM best_split bs
    $fmt$, 
        feature_col, cat_expr, join_sql
    );
    
    RETURN QUERY EXECUTE sql;
END;
$BODY$;