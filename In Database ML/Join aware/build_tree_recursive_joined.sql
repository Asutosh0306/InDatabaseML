CREATE OR REPLACE FUNCTION diss_joins.build_tree_recursive_joined(
	p_tree_id integer,
	p_parent_id integer,
	p_feature_cols text[],
	p_where_clause_sales text,
	p_where_clause_stores text,
	p_where_clause_items text,
	p_depth integer,
	p_max_depth integer,
	p_min_samples_split integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_node_id INTEGER;
    v_samples_count INTEGER;
    v_variance FLOAT8;
    v_mean_value FLOAT8;
    v_best_feature TEXT;
	v_best_split_type TEXT;
    v_best_table TEXT;
    v_best_variance_reduction FLOAT8;
    v_left_count BIGINT;
    v_right_count BIGINT;
    v_split_categories INT[];
    v_left_where_sales TEXT;
    v_left_where_stores TEXT;
    v_left_where_items TEXT;
    v_right_where_sales TEXT;
    v_right_where_stores TEXT;
    v_right_where_items TEXT;
    v_left_child_id INTEGER;
    v_right_child_id INTEGER;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Building joined node at depth % ===', p_depth;
    RAISE NOTICE 'WHERE sales: %', COALESCE(p_where_clause_sales, '1=1');
    RAISE NOTICE 'WHERE stores: %', COALESCE(p_where_clause_stores, '1=1');
    RAISE NOTICE 'WHERE items: %', COALESCE(p_where_clause_items, '1=1');

    -- Calculate node statistics
    SELECT samples_count, variance_value, mean_value
    INTO v_samples_count, v_variance, v_mean_value
    FROM diss_joins.calculate_node_stats_joined(
        COALESCE(p_where_clause_sales, '1=1'),
        COALESCE(p_where_clause_stores, '1=1'), 
        COALESCE(p_where_clause_items, '1=1')
    );

    RAISE NOTICE 'Node stats: samples=%, variance=%, mean=%',
        v_samples_count, v_variance, v_mean_value;

    -- Stopping criteria
    IF v_samples_count < p_min_samples_split OR p_depth >= p_max_depth THEN
        RAISE NOTICE 'Making leaf node.';
        
        INSERT INTO diss_joins.tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value,
            samples_count, variance, node_depth, 
            where_clause_sales, where_clause_stores, where_clause_items
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth,
            p_where_clause_sales, p_where_clause_stores, p_where_clause_items
        ) RETURNING node_id INTO v_node_id;

        RETURN v_node_id;
    END IF;

    -- Find best split
    SELECT 
        split_feature, split_table, split_type, variance_reduction, 
        left_count, right_count, split_categories
    INTO 
        v_best_feature, v_best_table, v_best_split_type, v_best_variance_reduction,
        v_left_count, v_right_count, v_split_categories
    FROM diss_joins.find_best_split_all_features_joined(
        p_feature_cols,
        COALESCE(p_where_clause_sales, '1=1'),
        COALESCE(p_where_clause_stores, '1=1'),
        COALESCE(p_where_clause_items, '1=1')
    ) LIMIT 1;

    IF v_best_feature IS NULL THEN
        RAISE NOTICE 'No valid split found. Creating leaf node.';
        
        INSERT INTO diss_joins.tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value,
            samples_count, variance, node_depth,
            where_clause_sales, where_clause_stores, where_clause_items
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth,
            p_where_clause_sales, p_where_clause_stores, p_where_clause_items
        ) RETURNING node_id INTO v_node_id;

        RETURN v_node_id;
    END IF;

    -- Check minimum samples constraint
    IF v_left_count < p_min_samples_split OR v_right_count < p_min_samples_split THEN
        RAISE NOTICE 'Split rejected due to min_samples_split constraint.';
        
        INSERT INTO diss_joins.tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value,
            samples_count, variance, node_depth,
            where_clause_sales, where_clause_stores, where_clause_items  
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth,
            p_where_clause_sales, p_where_clause_stores, p_where_clause_items
        ) RETURNING node_id INTO v_node_id;

        RETURN v_node_id;
    END IF;

    RAISE NOTICE 'SPLIT on %.% with categories % (gain=%)',
        v_best_table, v_best_feature, v_split_categories, v_best_variance_reduction;

    -- Create internal node
    INSERT INTO diss_joins.tree_nodes (
        tree_id, parent_id, is_leaf,
        split_feature, split_table, split_type, split_categories,
        samples_count, variance, node_depth,
        where_clause_sales, where_clause_stores, where_clause_items
    ) VALUES (
        p_tree_id, p_parent_id, FALSE,
        v_best_feature, v_best_table, v_best_split_type, v_split_categories,
        v_samples_count, v_variance, p_depth,
        p_where_clause_sales, p_where_clause_stores, p_where_clause_items
    ) RETURNING node_id INTO v_node_id;

    -- Build WHERE clauses for children based on which table has the split feature
    IF v_best_table = 'stores' THEN
        -- Split is on stores table
        v_left_where_sales := p_where_clause_sales;
        v_left_where_items := p_where_clause_items;
        v_right_where_sales := p_where_clause_sales;  
        v_right_where_items := p_where_clause_items;
        
        -- Modify stores WHERE clause
        IF p_where_clause_stores = '1=1' OR p_where_clause_stores = '' THEN
            v_left_where_stores := format('hashtext(st.%I::text) = ANY(%L)', 
                v_best_feature, v_split_categories);
            v_right_where_stores := format('hashtext(st.%I::text) != ALL(%L)', 
                v_best_feature, v_split_categories);
        ELSE
            v_left_where_stores := format('%s AND hashtext(st.%I::text) = ANY(%L)', 
                p_where_clause_stores, v_best_feature, v_split_categories);
            v_right_where_stores := format('%s AND hashtext(st.%I::text) != ALL(%L)', 
                p_where_clause_stores, v_best_feature, v_split_categories);
        END IF;
        
    ELSIF v_best_table = 'items' THEN
        -- Split is on items table
        v_left_where_sales := p_where_clause_sales;
        v_left_where_stores := p_where_clause_stores;
        v_right_where_sales := p_where_clause_sales;
        v_right_where_stores := p_where_clause_stores;
        
        -- Modify items WHERE clause  
        IF p_where_clause_items = '1=1' OR p_where_clause_items = '' THEN
            v_left_where_items := format('hashtext(i.%I::text) = ANY(%L)', 
                v_best_feature, v_split_categories);
            v_right_where_items := format('hashtext(i.%I::text) != ALL(%L)', 
                v_best_feature, v_split_categories);
        ELSE
            v_left_where_items := format('%s AND hashtext(i.%I::text) = ANY(%L)', 
                p_where_clause_items, v_best_feature, v_split_categories);
            v_right_where_items := format('%s AND hashtext(i.%I::text) != ALL(%L)', 
                p_where_clause_items, v_best_feature, v_split_categories);
        END IF;
        
    ELSE -- v_best_table = 'sales'
        -- Split is on sales table (like onpromotion)
        v_left_where_stores := p_where_clause_stores;
        v_left_where_items := p_where_clause_items;
        v_right_where_stores := p_where_clause_stores;
        v_right_where_items := p_where_clause_items;
        
        -- Modify sales WHERE clause
        IF p_where_clause_sales = '1=1' OR p_where_clause_sales = '' THEN
            v_left_where_sales := format('hashtext(s.%I::text) = ANY(%L)', 
                v_best_feature, v_split_categories);
            v_right_where_sales := format('hashtext(s.%I::text) != ALL(%L)', 
                v_best_feature, v_split_categories);
        ELSE
            v_left_where_sales := format('%s AND hashtext(s.%I::text) = ANY(%L)', 
                p_where_clause_sales, v_best_feature, v_split_categories);
            v_right_where_sales := format('%s AND hashtext(s.%I::text) != ALL(%L)', 
                p_where_clause_sales, v_best_feature, v_split_categories);
        END IF;
    END IF;

    -- Build left child
    RAISE NOTICE 'Building left child';
    v_left_child_id := diss_joins.build_tree_recursive_joined(
        p_tree_id, v_node_id, p_feature_cols,
        v_left_where_sales, v_left_where_stores, v_left_where_items,
        p_depth + 1, p_max_depth, p_min_samples_split
    );

    -- Build right child  
    RAISE NOTICE 'Building right child';
    v_right_child_id := diss_joins.build_tree_recursive_joined(
        p_tree_id, v_node_id, p_feature_cols,
        v_right_where_sales, v_right_where_stores, v_right_where_items,
        p_depth + 1, p_max_depth, p_min_samples_split
    );

    -- Update node with children IDs
    UPDATE diss_joins.tree_nodes
    SET left_child_id = v_left_child_id,
        right_child_id = v_right_child_id
    WHERE node_id = v_node_id;

    RETURN v_node_id;
END;
$BODY$;