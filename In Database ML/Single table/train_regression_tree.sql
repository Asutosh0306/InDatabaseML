CREATE OR REPLACE FUNCTION diss.train_regression_tree(
	tree_name text,
	target_column text,
	feature_columns text[],
	table_name text,
	max_depth integer,
	min_samples_split integer)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_tree_id INTEGER;
    v_root_id INTEGER;
BEGIN
    INSERT INTO decision_trees (
        tree_name, target_column, feature_columns, table_name, 
        max_depth, min_samples_split
    ) VALUES (
        tree_name, target_column, feature_columns, table_name,
        max_depth, min_samples_split
    ) RETURNING tree_id INTO v_tree_id;
    
    v_root_id := diss.build_tree_recursive(
        v_tree_id, NULL, table_name, target_column, feature_columns,
        '', 0, max_depth, min_samples_split
    );
    
    RETURN format('Tree "%s" trained successfully. Tree ID: %s, Root Node ID: %s', 
                  tree_name, v_tree_id, v_root_id);
END;
$BODY$;