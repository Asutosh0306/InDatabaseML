CREATE OR REPLACE FUNCTION diss._traverse_tree_from_json(
	p_tree_name text,
	p_row_json jsonb)
    RETURNS double precision
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  v_tree_id         INTEGER;
  v_current_node_id INTEGER;
  v_node            RECORD;
  v_feat_txt        TEXT;
  v_feat_cat        INT;
  v_feat_num        DOUBLE PRECISION;
BEGIN
  SELECT tree_id INTO v_tree_id
  FROM diss.decision_trees
  WHERE tree_name = p_tree_name;

  IF v_tree_id IS NULL THEN
    RAISE EXCEPTION 'Tree "%" not found in diss.decision_trees', p_tree_name;
  END IF;

  SELECT node_id INTO v_current_node_id
  FROM diss.tree_nodes
  WHERE tree_id = v_tree_id AND parent_id IS NULL;

  IF v_current_node_id IS NULL THEN
    RAISE EXCEPTION 'Root node not found for tree "%"', p_tree_name;
  END IF;

  LOOP
    SELECT *
      INTO v_node
    FROM diss.tree_nodes
    WHERE node_id = v_current_node_id;

    IF v_node.is_leaf THEN
      RETURN v_node.prediction_value;
    END IF;

    -- fetch the feature value from the JSON row using the split feature name
    v_feat_txt := p_row_json ->> v_node.split_feature;

    IF v_node.split_type = 'categorical' THEN
      v_feat_cat := v_feat_txt::INT;
      IF v_feat_cat = ANY(v_node.split_categories) THEN
        v_current_node_id := v_node.left_child_id;
      ELSE
        v_current_node_id := v_node.right_child_id;
      END IF;
    ELSE
      v_feat_num := v_feat_txt::FLOAT8;
      IF v_feat_num <= v_node.split_value THEN
        v_current_node_id := v_node.left_child_id;
      ELSE
        v_current_node_id := v_node.right_child_id;
      END IF;
    END IF;
  END LOOP;
END;
$BODY$;