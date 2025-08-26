CREATE OR REPLACE FUNCTION diss_joins.evaluate_tree_joined(
	p_tree_name text,
	p_test_table text)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_rows_added BIGINT;
    v_rel REGCLASS;
BEGIN
    v_rel := to_regclass(p_test_table);  -- resolves schema if on search_path
    IF v_rel IS NULL THEN
        RAISE EXCEPTION 'Relation % not found. Use a schema-qualified name like diss_joins.test_data, or add the schema to search_path.', p_test_table;
    END IF;

    EXECUTE format($Q$
        INSERT INTO diss_joins.evaluation_results_joined 
        (tree_name, store_nbr, item_nbr, predicted_value, true_value)
        SELECT
            %L AS tree_name,
            t.store_nbr,
            t.item_nbr,
            diss_joins.predict_tree_joined(%L, t.store_nbr, t.item_nbr) AS predicted_value,
            t.unit_sales AS true_value
        FROM %s t
        RETURNING 1
    $Q$, p_tree_name, p_tree_name, v_rel);

    GET DIAGNOSTICS v_rows_added = ROW_COUNT;
    RETURN v_rows_added;
END;
$BODY$;