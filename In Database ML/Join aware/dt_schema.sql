-- Tree storage schema
CREATE TABLE IF NOT EXISTS decision_trees (
    tree_id SERIAL PRIMARY KEY,
    tree_name VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    max_depth INTEGER DEFAULT 5,
    min_samples_split INTEGER DEFAULT 20,
    target_column VARCHAR(255),
    feature_columns TEXT[],
    table_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS tree_nodes (
    node_id SERIAL PRIMARY KEY,
    tree_id INTEGER REFERENCES decision_trees(tree_id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES tree_nodes(node_id),
    is_leaf BOOLEAN DEFAULT FALSE,
    split_feature VARCHAR(255),
    split_value FLOAT8,
    split_type VARCHAR(20), -- 'numeric' or 'categorical'
    left_child_id INTEGER,
    right_child_id INTEGER,
    prediction_value FLOAT8,
    samples_count INTEGER,
    variance FLOAT8,
    node_depth INTEGER DEFAULT 0,
    where_clause TEXT, -- accumulated WHERE clause to reach this node
    UNIQUE(tree_id, node_id)
);

CREATE INDEX idx_tree_nodes_tree_id ON tree_nodes(tree_id);
CREATE INDEX idx_tree_nodes_parent_id ON tree_nodes(parent_id);

-- Helper function to find best numeric split
CREATE OR REPLACE FUNCTION find_best_numeric_split(
    table_name TEXT,
    target_col TEXT,
    feature_col TEXT,
    where_clause TEXT DEFAULT ''
)
RETURNS TABLE(
    feature_name TEXT,
    threshold FLOAT8,
    variance_reduction FLOAT8,
    left_count BIGINT,
    right_count BIGINT
) AS $$
DECLARE
    sql_query TEXT;
    base_where TEXT;
BEGIN
    -- Build WHERE clause
    IF where_clause = '' THEN
        base_where := '';
    ELSE
        base_where := ' WHERE ' || where_clause;
    END IF;
    
    sql_query := format('
        WITH percentiles AS (
            SELECT 
                percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY %I::float8) as thresholds
            FROM %I%s
            WHERE %I IS NOT NULL
        ),
        thresholds AS (
            SELECT unnest(thresholds) as threshold
            FROM percentiles
        ),
        evaluations AS (
            SELECT 
                %L as feature_name,
                t.threshold,
                split_eval_agg(%I::float8, %I::float8, t.threshold) as variance_reduction,
                COUNT(CASE WHEN %I::float8 <= t.threshold THEN 1 END) as left_count,
                COUNT(CASE WHEN %I::float8 > t.threshold THEN 1 END) as right_count
            FROM %I, thresholds t%s
            GROUP BY t.threshold
        )
        SELECT * FROM evaluations
        ORDER BY variance_reduction DESC
        LIMIT 1',
        feature_col, table_name, base_where, feature_col,
        feature_col,
        target_col, feature_col, feature_col, feature_col,
        table_name, base_where
    );
    
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

-- Helper function to find best categorical split
CREATE OR REPLACE FUNCTION find_best_categorical_split(
    table_name TEXT,
    target_col TEXT,
    feature_col TEXT,
    where_clause TEXT DEFAULT ''
)
RETURNS TABLE(
    feature_name TEXT,
    variance_reduction FLOAT8,
    n_categories INTEGER
) AS $$
DECLARE
    sql_query TEXT;
    base_where TEXT;
BEGIN
    IF where_clause = '' THEN
        base_where := '';
    ELSE
        base_where := ' WHERE ' || where_clause;
    END IF;
    
    sql_query := format('
        SELECT 
            %L as feature_name,
            categorical_split_agg(%I::float8, %I::int4) as variance_reduction,
            COUNT(DISTINCT %I)::integer as n_categories
        FROM %I%s',
        feature_col,
        target_col, feature_col, feature_col,
        table_name, base_where
    );
    
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

-- Main function to find best split across all features
CREATE OR REPLACE FUNCTION find_best_split_all_features(
    table_name TEXT,
    target_col TEXT,
    feature_cols TEXT[],
    where_clause TEXT DEFAULT ''
)
RETURNS TABLE(
    feature_name TEXT,
    split_value FLOAT8,
    split_type TEXT,
    variance_reduction FLOAT8,
    left_count BIGINT,
    right_count BIGINT
) AS $$
DECLARE
    feature TEXT;
    best_feature TEXT;
    best_threshold FLOAT8;
    best_variance_reduction FLOAT8 := -1;
    best_left_count BIGINT;
    best_right_count BIGINT;
    best_split_type TEXT;
    temp_feature TEXT;
    temp_threshold FLOAT8;
    temp_variance_reduction FLOAT8;
    temp_left_count BIGINT;
    temp_right_count BIGINT;
    cat_variance_reduction FLOAT8;
    cat_n_categories INTEGER;
BEGIN
    -- Check each feature
    FOREACH feature IN ARRAY feature_cols
    LOOP
        -- Try numeric split first
        BEGIN
            SELECT 
                f.feature_name, f.threshold, f.variance_reduction, f.left_count, f.right_count
            INTO 
                temp_feature, temp_threshold, temp_variance_reduction, temp_left_count, temp_right_count
            FROM find_best_numeric_split(table_name, target_col, feature, where_clause) f;
            
            IF temp_variance_reduction > best_variance_reduction THEN
                best_feature := feature;
                best_threshold := temp_threshold;
                best_variance_reduction := temp_variance_reduction;
                best_left_count := temp_left_count;
                best_right_count := temp_right_count;
                best_split_type := 'numeric';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Feature might be categorical, skip numeric evaluation
        END;
        
        -- Try categorical split
        BEGIN
            SELECT 
                f.feature_name, f.variance_reduction, f.n_categories
            INTO 
                temp_feature, cat_variance_reduction, cat_n_categories
            FROM find_best_categorical_split(table_name, target_col, feature, where_clause) f;
            
            IF cat_variance_reduction > best_variance_reduction THEN
                best_feature := feature;
                best_threshold := NULL;
                best_variance_reduction := cat_variance_reduction;
                best_left_count := NULL;
                best_right_count := NULL;
                best_split_type := 'categorical';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Skip if categorical evaluation fails
        END;
    END LOOP;
    
    IF best_variance_reduction > 0 THEN
        RETURN QUERY SELECT 
            best_feature,
            best_threshold,
            best_split_type,
            best_variance_reduction,
            best_left_count,
            best_right_count;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Build tree recursively
CREATE OR REPLACE FUNCTION build_tree_recursive(
    p_tree_id INTEGER,
    p_parent_id INTEGER,
    p_table_name TEXT,
    p_target_col TEXT,
    p_feature_cols TEXT[],
    p_where_clause TEXT,
    p_depth INTEGER,
    p_max_depth INTEGER,
    p_min_samples_split INTEGER
)
RETURNS INTEGER AS $$
DECLARE
    v_node_id INTEGER;
    v_samples_count INTEGER;
    v_variance FLOAT8;
    v_mean_value FLOAT8;
    v_best_feature TEXT;
    v_best_threshold FLOAT8;
    v_best_split_type TEXT;
    v_best_variance_reduction FLOAT8;
    v_left_count BIGINT;
    v_right_count BIGINT;
    v_left_where TEXT;
    v_right_where TEXT;
    v_left_child_id INTEGER;
    v_right_child_id INTEGER;
    sql_query TEXT;
BEGIN
    -- Calculate node statistics
    sql_query := format('
        SELECT 
            COUNT(*)::integer,
            variance_agg(%I::float8),
            AVG(%I::float8)
        FROM %I
        WHERE %s',
        p_target_col, p_target_col,
        p_table_name,
        CASE WHEN p_where_clause = '' THEN '1=1' ELSE p_where_clause END
    );
    
    EXECUTE sql_query INTO v_samples_count, v_variance, v_mean_value;
    
    -- Check stopping criteria
    IF p_depth >= p_max_depth OR v_samples_count < p_min_samples_split OR v_variance < 0.001 THEN
        -- Create leaf node
        INSERT INTO tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value, 
            samples_count, variance, node_depth, where_clause
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth, p_where_clause
        ) RETURNING node_id INTO v_node_id;
        
        RETURN v_node_id;
    END IF;
    
    -- Find best split
    SELECT 
        feature_name, split_value, split_type, variance_reduction, left_count, right_count
    INTO 
        v_best_feature, v_best_threshold, v_best_split_type, 
        v_best_variance_reduction, v_left_count, v_right_count
    FROM find_best_split_all_features(p_table_name, p_target_col, p_feature_cols, p_where_clause);
    
    -- If no good split found, create leaf
    IF v_best_feature IS NULL OR v_best_variance_reduction <= 0 THEN
        INSERT INTO tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value, 
            samples_count, variance, node_depth, where_clause
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth, p_where_clause
        ) RETURNING node_id INTO v_node_id;
        
        RETURN v_node_id;
    END IF;
    
    -- Create internal node
    INSERT INTO tree_nodes (
        tree_id, parent_id, is_leaf, split_feature, split_value, split_type,
        samples_count, variance, node_depth, where_clause
    ) VALUES (
        p_tree_id, p_parent_id, FALSE, v_best_feature, v_best_threshold, v_best_split_type,
        v_samples_count, v_variance, p_depth, p_where_clause
    ) RETURNING node_id INTO v_node_id;
    
    -- Build WHERE clauses for children
    IF v_best_split_type = 'numeric' THEN
        IF p_where_clause = '' THEN
            v_left_where := format('%I <= %L', v_best_feature, v_best_threshold);
            v_right_where := format('%I > %L', v_best_feature, v_best_threshold);
        ELSE
            v_left_where := format('%s AND %I <= %L', p_where_clause, v_best_feature, v_best_threshold);
            v_right_where := format('%s AND %I > %L', p_where_clause, v_best_feature, v_best_threshold);
        END IF;
    ELSE
        -- For categorical splits, treat as "feature = most common category" vs "others"
        -- This is a simplification - ideally we'd partition categories optimally
        -- For now, create a leaf node for categorical splits
        INSERT INTO tree_nodes (
            tree_id, parent_id, is_leaf, prediction_value, 
            samples_count, variance, node_depth, where_clause
        ) VALUES (
            p_tree_id, p_parent_id, TRUE, v_mean_value,
            v_samples_count, v_variance, p_depth, p_where_clause
        ) RETURNING node_id INTO v_node_id;
        
        RETURN v_node_id;
    END IF;
    
    -- Build left subtree
    v_left_child_id := build_tree_recursive(
        p_tree_id, v_node_id, p_table_name, p_target_col, p_feature_cols,
        v_left_where, p_depth + 1, p_max_depth, p_min_samples_split
    );
    
    -- Build right subtree
    v_right_child_id := build_tree_recursive(
        p_tree_id, v_node_id, p_table_name, p_target_col, p_feature_cols,
        v_right_where, p_depth + 1, p_max_depth, p_min_samples_split
    );
    
    -- Update node with children IDs
    UPDATE tree_nodes 
    SET left_child_id = v_left_child_id, right_child_id = v_right_child_id
    WHERE node_id = v_node_id;
    
    RETURN v_node_id;
END;
$$ LANGUAGE plpgsql;

-- Main training function
CREATE OR REPLACE FUNCTION train_regression_tree(
    tree_name TEXT,
    table_name TEXT,
    target_column TEXT,
    feature_columns TEXT[],
    max_depth INTEGER DEFAULT 5,
    min_samples_split INTEGER DEFAULT 20
)
RETURNS TEXT AS $$
DECLARE
    v_tree_id INTEGER;
    v_root_id INTEGER;
BEGIN
    -- Create tree record
    INSERT INTO decision_trees (
        tree_name, target_column, feature_columns, table_name, 
        max_depth, min_samples_split
    ) VALUES (
        tree_name, target_column, feature_columns, table_name,
        max_depth, min_samples_split
    ) RETURNING tree_id INTO v_tree_id;
    
    -- Build tree recursively
    v_root_id := build_tree_recursive(
        v_tree_id, NULL, table_name, target_column, feature_columns,
        '', 0, max_depth, min_samples_split
    );
    
    RETURN format('Tree "%s" trained successfully. Tree ID: %s, Root Node ID: %s', 
                  tree_name, v_tree_id, v_root_id);
END;
$$ LANGUAGE plpgsql;

-- Prediction function for single row
CREATE OR REPLACE FUNCTION predict_tree(
    p_tree_name TEXT,
    p_feature_values JSONB
)
RETURNS FLOAT8 AS $$
DECLARE
    v_tree_id INTEGER;
    v_current_node_id INTEGER;
    v_node RECORD;
    v_feature_value FLOAT8;
BEGIN
    -- Get tree ID
    SELECT tree_id INTO v_tree_id 
    FROM decision_trees 
    WHERE tree_name = p_tree_name;
    
    IF v_tree_id IS NULL THEN
        RAISE EXCEPTION 'Tree "%" not found', p_tree_name;
    END IF;
    
    -- Start at root
    SELECT node_id INTO v_current_node_id
    FROM tree_nodes
    WHERE tree_id = v_tree_id AND parent_id IS NULL;
    
    -- Traverse tree
    LOOP
        SELECT * INTO v_node
        FROM tree_nodes
        WHERE node_id = v_current_node_id;
        
        -- If leaf, return prediction
        IF v_node.is_leaf THEN
            RETURN v_node.prediction_value;
        END IF;
        
        -- Get feature value
        v_feature_value := (p_feature_values->>v_node.split_feature)::float8;
        
        -- Decide which child to visit
        IF v_feature_value <= v_node.split_value THEN
            v_current_node_id := v_node.left_child_id;
        ELSE
            v_current_node_id := v_node.right_child_id;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Batch prediction function
CREATE OR REPLACE FUNCTION predict_tree_batch(
    p_tree_name TEXT,
    p_table_name TEXT,
    p_id_column TEXT DEFAULT 'id'
)
RETURNS TABLE(
    row_id BIGINT,
    prediction FLOAT8
) AS $$
DECLARE
    v_tree_id INTEGER;
    v_feature_columns TEXT[];
    sql_query TEXT;
BEGIN
    -- Get tree info
    SELECT tree_id, feature_columns 
    INTO v_tree_id, v_feature_columns
    FROM decision_trees 
    WHERE tree_name = p_tree_name;
    
    -- Build dynamic query for batch prediction
    sql_query := format('
        WITH RECURSIVE predictions AS (
            -- Start with all rows at root
            SELECT 
                t.%I as row_id,
                n.node_id,
                n.is_leaf,
                n.prediction_value,
                n.split_feature,
                n.split_value,
                n.left_child_id,
                n.right_child_id,
                t.*
            FROM %I t
            CROSS JOIN tree_nodes n
            WHERE n.tree_id = %L AND n.parent_id IS NULL
            
            UNION ALL
            
            -- Traverse tree
            SELECT 
                p.row_id,
                CASE 
                    WHEN p.split_feature IS NULL THEN p.node_id
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.node_id
                    ELSE n_right.node_id
                END as node_id,
                CASE 
                    WHEN p.split_feature IS NULL THEN p.is_leaf
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.is_leaf
                    ELSE n_right.is_leaf
                END as is_leaf,
                CASE 
                    WHEN p.split_feature IS NULL THEN p.prediction_value
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.prediction_value
                    ELSE n_right.prediction_value
                END as prediction_value,
                CASE 
                    WHEN p.split_feature IS NULL THEN NULL
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.split_feature
                    ELSE n_right.split_feature
                END as split_feature,
                CASE 
                    WHEN p.split_feature IS NULL THEN NULL
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.split_value
                    ELSE n_right.split_value
                END as split_value,
                CASE 
                    WHEN p.split_feature IS NULL THEN NULL
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.left_child_id
                    ELSE n_right.left_child_id
                END as left_child_id,
                CASE 
                    WHEN p.split_feature IS NULL THEN NULL
                    WHEN (p.%I)::float8 <= p.split_value THEN n_left.right_child_id
                    ELSE n_right.right_child_id
                END as right_child_id,
                p.*
            FROM predictions p
            LEFT JOIN tree_nodes n_left ON n_left.node_id = p.left_child_id
            LEFT JOIN tree_nodes n_right ON n_right.node_id = p.right_child_id
            WHERE NOT p.is_leaf
        )
        SELECT row_id, prediction_value as prediction
        FROM predictions
        WHERE is_leaf',
        p_id_column, p_table_name, v_tree_id,
        'split_feature', 'split_feature', 'split_feature', 
        'split_feature', 'split_feature', 'split_feature', 'split_feature'
    );
    
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

-- Function to visualize tree structure
CREATE OR REPLACE FUNCTION visualize_tree(p_tree_name TEXT)
RETURNS TABLE(
    level INTEGER,
    node_path TEXT,
    node_info TEXT
) AS $$
WITH RECURSIVE tree_viz AS (
    SELECT 
        0 as level,
        ARRAY[node_id] as path,
        node_id,
        parent_id,
        is_leaf,
        split_feature,
        split_value,
        prediction_value,
        samples_count,
        variance
    FROM tree_nodes
    WHERE tree_id = (SELECT tree_id FROM decision_trees WHERE tree_name = p_tree_name)
        AND parent_id IS NULL
    
    UNION ALL
    
    SELECT 
        tv.level + 1,
        tv.path || n.node_id,
        n.node_id,
        n.parent_id,
        n.is_leaf,
        n.split_feature,
        n.split_value,
        n.prediction_value,
        n.samples_count,
        n.variance
    FROM tree_viz tv
    JOIN tree_nodes n ON n.parent_id = tv.node_id
)
SELECT 
    level,
    repeat('  ', level) || 'Node ' || node_id as node_path,
    CASE 
        WHEN is_leaf THEN 
            format('LEAF: prediction=%.2f, samples=%s, variance=%.2f', 
                   prediction_value, samples_count, variance)
        ELSE 
            format('SPLIT: %s <= %.2f, samples=%s, variance=%.2f', 
                   split_feature, split_value, samples_count, variance)
    END as node_info
FROM tree_viz
ORDER BY path;
$$ LANGUAGE sql;