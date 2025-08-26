CREATE OR REPLACE FUNCTION diss.evaluate_treee(
	p_tree_name text,
	p_target_column text,
	p_source_sql text)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  v_insert_sql TEXT;
  v_rows_added BIGINT;
BEGIN
  -- Build one INSERT ... SELECT that:
  --  - converts each source row to JSON
  --  - predicts with _traverse_tree_from_json(...)
  --  - extracts true_value from the same JSON by p_target_column
  v_insert_sql := format($Q$
    INSERT INTO diss.evaluation_results (tree_name, predicted_value, true_value)
    SELECT
      %L AS tree_name,
      diss._traverse_tree_from_json(%L, row_to_json(s)::jsonb) AS predicted_value,
      (row_to_json(s)::jsonb ->> %L)::float8                  AS true_value
    FROM (%s) AS s
    RETURNING 1
  $Q$, p_tree_name, p_tree_name, p_target_column, p_source_sql);

  -- Execute and count inserted rows
  EXECUTE v_insert_sql;

  GET DIAGNOSTICS v_rows_added = ROW_COUNT;
  RETURN v_rows_added;
END;
$BODY$;