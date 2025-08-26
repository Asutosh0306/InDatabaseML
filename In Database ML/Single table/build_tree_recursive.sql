CREATE OR REPLACE FUNCTION diss.build_tree_recursive(
	p_tree_id integer,
	p_parent_id integer,
	p_table_name text,
	p_target_col text,
	p_feature_cols text[],
	p_where_clause text,
	p_depth integer,
	p_max_depth integer,
	p_min_samples_split integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_node_id                 INTEGER;
    v_samples_count           INTEGER;
    v_variance                FLOAT8;
    v_mean_value              FLOAT8;

    v_best_feature            TEXT;
    v_best_threshold          FLOAT8;
    v_best_split_type         TEXT;
    v_best_variance_reduction FLOAT8;
    v_left_count              BIGINT;
    v_right_count             BIGINT;
    v_split_categories        INT[];

    v_left_where              TEXT;
    v_right_where             TEXT;
    v_left_child_id           INTEGER;
    v_right_child_id          INTEGER;

    sql_query                 TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Building node at depth % ===', p_depth;
    RAISE NOTICE 'WHERE clause: %', COALESCE(p_where_clause, '(root)');
    RAISE NOTICE 'Available features: %', p_feature_cols;

    -- Compute node stats
    sql_query := format($q$
        SELECT 
            COUNT(*)::integer,
            CASE WHEN COUNT(*) > 1 THEN variance_agg(%I::float8) ELSE 0.0 END,
            AVG(%I::float8)
        FROM %I
        WHERE %s
    $q$,
        p_target_col, p_target_col,
        p_table_name,
        CASE WHEN p_where_clause = '' THEN '1=1' ELSE p_where_clause END
    );

    EXECUTE sql_query INTO v_samples_count, v_variance, v_mean_value;

    RAISE NOTICE 'Node stats: samples=% variance=%, mean=%',
                 v_samples_count, v_variance, v_mean_value;

    -- Stopping criteria
    IF v_samples_count < p_min_samples_split OR p_depth >= p_max_depth THEN
        RAISE NOTICE 'Making leaf node.';
        INSERT INTO tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value,
            samples_count, variance, node_depth, where_clause
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth, p_where_clause
        )
        RETURNING node_id INTO v_node_id;

        RETURN v_node_id;
    END IF;

    -- Find the best split across all features
    v_best_feature := NULL;
    SELECT
        s.split_feature, s.split_value, s.split_type, s.variance_reduction,
        s.left_count, s.right_count, s.split_categories
    INTO
        v_best_feature, v_best_threshold, v_best_split_type, v_best_variance_reduction,
        v_left_count, v_right_count, v_split_categories
    FROM diss.find_best_split_all_features(
        p_table_name, p_target_col, p_feature_cols,
        CASE WHEN p_where_clause = '' THEN '1=1' ELSE p_where_clause END
    ) AS s
    LIMIT 1;

    IF v_best_feature IS NULL THEN
        RAISE NOTICE 'No valid split found. Creating leaf node.';
        INSERT INTO tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value,
            samples_count, variance, node_depth, where_clause
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth, p_where_clause
        )
        RETURNING node_id INTO v_node_id;
        RETURN v_node_id;
    END IF;

    IF v_best_split_type = 'numeric' THEN
        IF v_left_count  < p_min_samples_split OR
           v_right_count < p_min_samples_split THEN
            RAISE NOTICE 'Split rejected due to min_samples_split constraint. Creating leaf node.';
            INSERT INTO tree_nodes (
                tree_id, parent_id, is_leaf, prediction_value,
                samples_count, variance, node_depth, where_clause
            ) VALUES (
                p_tree_id, p_parent_id, TRUE, v_mean_value,
                v_samples_count, v_variance, p_depth, p_where_clause
            )
            RETURNING node_id INTO v_node_id;
            RETURN v_node_id;
        END IF;
    END IF;

    RAISE NOTICE 'SPLIT on % @ % (gain=%)', 
                 v_best_feature, v_best_threshold, v_best_variance_reduction;

    -- Create internal node; persist split_categories for categorical splits
    INSERT INTO tree_nodes (
        tree_id, parent_id, is_leaf,
        split_feature, split_value, split_type, split_categories,
        samples_count, variance, node_depth, where_clause
    ) VALUES (
        p_tree_id, p_parent_id, FALSE,
        v_best_feature, v_best_threshold, v_best_split_type,
        CASE WHEN v_best_split_type = 'categorical' THEN v_split_categories ELSE NULL END,
        v_samples_count, v_variance, p_depth, p_where_clause
    )
    RETURNING node_id INTO v_node_id;

    -- Build WHERE clauses for children (preserve existing filters)
    IF v_best_split_type = 'numeric' THEN
        IF p_where_clause = '' THEN
            v_left_where  := format('%I <= %s', v_best_feature, v_best_threshold);
            v_right_where := format('%I >  %s', v_best_feature, v_best_threshold);
        ELSE
            v_left_where  := format('%s AND %I <= %s', p_where_clause, v_best_feature, v_best_threshold);
            v_right_where := format('%s AND %I >  %s', p_where_clause, v_best_feature, v_best_threshold);
        END IF;
    ELSE
        IF p_where_clause = '' THEN
            v_left_where  := format('%I = ANY(%L)',  v_best_feature, v_split_categories);
            v_right_where := format('%I <> ALL(%L)', v_best_feature, v_split_categories);
        ELSE
            v_left_where  := format('%s AND %I = ANY(%L)',  p_where_clause, v_best_feature, v_split_categories);
            v_right_where := format('%s AND %I <> ALL(%L)', p_where_clause, v_best_feature, v_split_categories);
        END IF;
    END IF;

    -- Recurse
    RAISE NOTICE 'Building left child with WHERE: %', v_left_where;
    v_left_child_id := diss.build_tree_recursive(
        p_tree_id, v_node_id, p_table_name, p_target_col,
        p_feature_cols, v_left_where,
        p_depth + 1, p_max_depth, p_min_samples_split
    );

    RAISE NOTICE 'Building right child with WHERE: %', v_right_where;
    v_right_child_id := diss.build_tree_recursive(
        p_tree_id, v_node_id, p_table_name, p_target_col,
        p_feature_cols, v_right_where,
        p_depth + 1, p_max_depth, p_min_samples_split
    );

    UPDATE tree_nodes
       SET left_child_id  = v_left_child_id,
           right_child_id = v_right_child_id
     WHERE node_id = v_node_id;

    RETURN v_node_id;
END;
$BODY$;