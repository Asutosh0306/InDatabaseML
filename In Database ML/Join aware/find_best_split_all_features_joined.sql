CREATE OR REPLACE FUNCTION diss_joins.find_best_split_all_features_joined(
	feature_cols text[],
	where_clause_sales text DEFAULT '1=1'::text,
	where_clause_stores text DEFAULT '1=1'::text,
	where_clause_items text DEFAULT '1=1'::text)
    RETURNS TABLE(split_feature text, split_table text, split_value double precision, split_type text, variance_reduction double precision, left_count bigint, right_count bigint, split_categories integer[]) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    feature TEXT;
    table_name TEXT;
    col_name TEXT;
    best_feature TEXT := NULL;
    best_table TEXT := NULL;
    best_variance DOUBLE PRECISION := -1;
    best_cats INT[] := NULL;
    best_left BIGINT := NULL;
    best_right BIGINT := NULL;
    tmp_cat_val INT;
    tmp_var DOUBLE PRECISION;
    tmp_left BIGINT;
    tmp_right BIGINT;
    parts TEXT[];
BEGIN
    FOREACH feature IN ARRAY feature_cols LOOP
        -- Parse table.column format
        parts := string_to_array(feature, '.');
        IF array_length(parts, 1) = 2 THEN
            table_name := parts[1];
            col_name := parts[2];
        ELSE
            CONTINUE; -- Skip malformed feature names
        END IF;
        
        -- Only process categorical features (all features in this project are categorical)
        FOR tmp_cat_val, tmp_var, tmp_left, tmp_right IN
            SELECT 
                f.category_value, 
                f.variance_reduction,
                f.left_count,
                f.right_count
            FROM diss_joins.find_best_categorical_split_joined(
                col_name, table_name, 
                where_clause_sales, where_clause_stores, where_clause_items
            ) AS f
        LOOP
            IF tmp_var > best_variance THEN
                best_feature := col_name;
                best_table := table_name;
                best_variance := tmp_var;
                best_left := tmp_left;
                best_right := tmp_right;
                best_cats := ARRAY[tmp_cat_val];
            END IF;
        END LOOP;
    END LOOP;

    IF best_variance > 0 THEN
        RETURN QUERY SELECT
            best_feature,
            best_table,
            NULL::DOUBLE PRECISION, -- split_value (not used for categorical)
            'categorical'::TEXT,
            best_variance,
            best_left,
            best_right,
            best_cats;
    END IF;
END;
$BODY$;