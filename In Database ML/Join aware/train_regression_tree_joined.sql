CREATE OR REPLACE FUNCTION diss_joins.train_regression_tree_joined(
	tree_name text,
	target_column text DEFAULT 'unit_sales'::text,
	max_depth integer DEFAULT 5,
	min_samples_split integer DEFAULT 20)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_tree_id INTEGER;
    v_root_id INTEGER;
    v_feature_cols TEXT[] := ARRAY[
		'stores.store_nbr',
		'items.item_nbr',
        'stores.cluster',
        'items.family', 
        'items.class'
    ]; -- Only the features we want to use
BEGIN
    -- Create tree record
    INSERT INTO diss_joins.decision_trees (
        tree_name, target_column, feature_columns, base_table,
        max_depth, min_samples_split, join_info
    ) VALUES (
        tree_name, target_column, v_feature_cols, 'train_sales',
        max_depth, min_samples_split, 
        '{"joins": ["stores ON store_nbr", "items ON item_nbr"]}'::jsonb
    ) RETURNING tree_id INTO v_tree_id;

    -- Build tree recursively with joins
    v_root_id := diss_joins.build_tree_recursive_joined(
        v_tree_id, NULL, v_feature_cols,
        '1=1', '1=1', '1=1', -- Initial WHERE clauses  
        0, max_depth, min_samples_split
    );

    RETURN format('Joined tree "%s" trained successfully. Tree ID: %s, Root Node ID: %s', 
        tree_name, v_tree_id, v_root_id);
END;
$BODY$;