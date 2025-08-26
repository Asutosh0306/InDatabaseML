CREATE OR REPLACE FUNCTION diss.find_best_split_all_features(
	table_name text,
	target_col text,
	feature_cols text[],
	where_clause text)
    RETURNS TABLE(split_feature text, split_value double precision, split_type text, variance_reduction double precision, left_count bigint, right_count bigint, split_categories integer[]) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
  feature       TEXT;
  best_feature       TEXT    := NULL;
  best_threshold     DOUBLE PRECISION := NULL;
  best_variance      DOUBLE PRECISION := -1;
  best_left          BIGINT  := NULL;
  best_right         BIGINT  := NULL;
  best_type          TEXT    := NULL;
  best_cats          INT[]   := NULL;

  tmp_thresh   DOUBLE PRECISION;
  tmp_var      DOUBLE PRECISION;
  tmp_left     BIGINT;
  tmp_right    BIGINT;

  tmp_cat_val  INT;
  tmp_cat_var  DOUBLE PRECISION;

  wc TEXT := CASE WHEN where_clause = '' THEN '1=1' ELSE where_clause END;
BEGIN
  FOR feature IN SELECT unnest(feature_cols)
  LOOP
    IF feature NOT IN ('onpromotion') THEN
      SELECT
        s.threshold,
        s.variance_reduction,
        s.left_count,
        s.right_count
      INTO
        tmp_thresh, tmp_var, tmp_left, tmp_right
      FROM diss.find_all_numeric_splits_one_pass(
             table_name, target_col, feature, wc
           ) AS s
      ORDER BY s.variance_reduction DESC
      LIMIT 1;

      IF tmp_var > best_variance THEN
        best_feature   := feature;
        best_threshold := tmp_thresh;
        best_variance  := tmp_var;
        best_left      := tmp_left;
        best_right     := tmp_right;
        best_type      := 'numeric';
        best_cats      := NULL;
      END IF;

    ELSE
      FOR tmp_cat_val, tmp_cat_var IN
        SELECT f.category_value, f.variance_reduction
        FROM diss.find_best_categorical_split(
               table_name, target_col, feature, wc
             ) AS f
      LOOP
        IF tmp_cat_var > best_variance THEN
          best_feature      := feature;
          best_threshold    := NULL;
          best_variance     := tmp_cat_var;
          best_left         := NULL;
          best_right        := NULL;
          best_type         := 'categorical';
          best_cats         := ARRAY[tmp_cat_val];
        END IF;
      END LOOP;

    END IF;
  END LOOP;

  IF best_variance > 0 THEN
    RETURN QUERY SELECT
      best_feature,
      best_threshold,
      best_type,
      best_variance,
      best_left,
      best_right,
      best_cats;
  END IF;

  RETURN;
END;
$BODY$;