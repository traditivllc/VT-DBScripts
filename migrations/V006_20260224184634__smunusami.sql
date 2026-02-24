CREATE PROCEDURE `trax2022_get_lmia_province`(
    IN p_start INT,
    IN p_length INT,
    IN p_search VARCHAR(200),
    IN p_order_column VARCHAR(50),
    IN p_order_dir VARCHAR(4)
)
BEGIN
    DECLARE sql_base TEXT;
    DECLARE sql_where TEXT DEFAULT ' WHERE 1=1 ';
    DECLARE full_sql TEXT;
    SET p_search = REPLACE(p_search,"'","''");

    SET sql_base = 
        " FROM trax2022_lmiaprovince tl ";
    
    IF p_search IS NOT NULL AND p_search != '' THEN
        SET sql_where = CONCAT(sql_where,
            " AND (tl.province LIKE '%", p_search, "%')");
    END IF;
    
    SET @query = CONCAT(
        "SELECT 
            tl.provinceid,
            tl.province,
            tl.isactive,
			tl.province_slug,
            tl.created_date,
            (SELECT COUNT(*) ", sql_base, sql_where, ") AS total_count ",
        sql_base, sql_where,
        " ORDER BY ", p_order_column, " ", p_order_dir,
        " LIMIT ", p_start, ", ", p_length
    );

    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
end