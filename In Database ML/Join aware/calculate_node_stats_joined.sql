CREATE OR REPLACE FUNCTION diss_joins.calculate_node_stats_joined(
	where_clause_sales text DEFAULT '1=1'::text,
	where_clause_stores text DEFAULT '1=1'::text,
	where_clause_items text DEFAULT '1=1'::text)
    RETURNS TABLE(samples_count integer, variance_value double precision, mean_value double precision) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    sql_query TEXT;
BEGIN
    sql_query := format($q$
        SELECT
            COUNT(*)::integer,
            CASE WHEN COUNT(*) > 1 THEN variance_agg(s.unit_sales::float8) ELSE 0.0 END,
            AVG(s.unit_sales::float8)
        FROM diss_joins.train_sales s
        INNER JOIN diss_joins.stores st ON s.store_nbr = st.store_nbr
        INNER JOIN diss_joins.items i ON s.item_nbr = i.item_nbr
        WHERE (%s) AND (%s) AND (%s)
    $q$, where_clause_sales, where_clause_stores, where_clause_items);
    
    RETURN QUERY EXECUTE sql_query;
END;
$BODY$;