CREATE OR REPLACE FUNCTION diss.predict_tree(
	p_tree_name text,
	p_feature_values jsonb)
    RETURNS double precision
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_tree_id           INTEGER;
    v_current_node_id   INTEGER;
    v_node              RECORD;
    v_feature_text      TEXT;
    v_feature_cat       INT;
    v_feature_value     FLOAT8;
BEGIN
    SELECT tree_id
      INTO v_tree_id
      FROM decision_trees
     WHERE tree_name = p_tree_name;

    IF v_tree_id IS NULL THEN
        RAISE EXCEPTION 'Tree "%" not found', p_tree_name;
    END IF;

    SELECT node_id
      INTO v_current_node_id
      FROM tree_nodes
     WHERE tree_id = v_tree_id
       AND parent_id IS NULL;

    LOOP
        SELECT *
          INTO v_node
          FROM tree_nodes
         WHERE node_id = v_current_node_id;

        IF v_node.is_leaf THEN
            RETURN v_node.prediction_value;
        END IF;

        v_feature_text := p_feature_values ->> v_node.split_feature;

        IF v_node.split_type = 'categorical' THEN
            -- Robustly coerce the incoming value to the same int coding used in training
            IF v_feature_text IS NULL THEN
                v_feature_cat := NULL;
            ELSIF lower(v_feature_text) IN ('t','true') THEN
                v_feature_cat := 1;
            ELSIF lower(v_feature_text) IN ('f','false') THEN
                v_feature_cat := 0;
            ELSIF v_feature_text ~ '^-?[0-9]+$' THEN
                v_feature_cat := v_feature_text::int;
            ELSE
                v_feature_cat := hashtext(v_feature_text);
            END IF;

            IF v_feature_cat = ANY(v_node.split_categories) THEN
                v_current_node_id := v_node.left_child_id;
            ELSE
                v_current_node_id := v_node.right_child_id;
            END IF;

        ELSE
            v_feature_value := v_feature_text::float8;
            IF v_feature_value <= v_node.split_value THEN
                v_current_node_id := v_node.left_child_id;
            ELSE
                v_current_node_id := v_node.right_child_id;
            END IF;
        END IF;
    END LOOP;
END;
$BODY$;