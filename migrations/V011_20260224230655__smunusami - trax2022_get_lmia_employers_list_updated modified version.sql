CREATE PROCEDURE trax2022_get_lmia_employers_list_updated(
IN p_start INT,
    IN p_length INT,
    IN p_search VARCHAR(200),
    IN p_order_column VARCHAR(50),
    IN p_order_dir VARCHAR(4),
    IN p_ddl_occupation VARCHAR(5000),
    IN p_ddl_province VARCHAR(5000),
    IN p_search_employer VARCHAR(500),
    IN p_search_address VARCHAR(500)
    )
begin
	DECLARE sql_base TEXT;
    DECLARE sql_where TEXT DEFAULT ' WHERE 1=1 ';
   
    -- Escape single quotes
    SET p_search = REPLACE(p_search,"'","''");
    SET p_search_employer = REPLACE(p_search_employer,"'","''");
    SET p_search_address = REPLACE(p_search_address,"'","''");
    SET p_ddl_occupation = REPLACE(p_ddl_occupation,"'","''");
    SET p_ddl_province = REPLACE(p_ddl_province,"'","''");
   
    SET sql_base = 
        " FROM trax2022_lmiaemployerapprovals tl 
          INNER JOIN trax2022_lmiaemployer tl6 ON tl6.employerid = tl.employerid 
          INNER JOIN trax2022_lmiaemployeroccupations tl3 ON tl.leoid = tl3.leoid AND tl3.employerid = tl6.employerid 
          INNER JOIN trax2022_lmiaemployerstreams tl4 ON tl.lesid = tl4.lesid AND tl4.employerid = tl6.employerid 
          INNER JOIN trax2022_lmiaemployerprovences tl5 ON tl.lepid = tl5.lepid AND tl5.employerid = tl6.employerid 
          INNER JOIN trax2022_lmiaprovince tl7 ON tl7.provinceid = tl5.provinceid 
          INNER JOIN trax2022_lmiaprogramstream tl8 ON tl8.streamid  = tl4.streamid 
          INNER JOIN trax2022_lmiaoccupation tl9 ON tl9.occupationid = tl3.occupationid 
          INNER JOIN trax2022_lmiayear tl10 ON tl10.yearid = tl.yearid ";

    -- Filters
    IF p_search IS NOT NULL AND p_search != '' THEN
        SET sql_where = CONCAT(sql_where,
            " AND (tl6.employer LIKE '%", p_search, "%'",
            " OR tl6.address LIKE '%", p_search, "%'",
            " OR tl9.noc_code LIKE '%", p_search, "%'",
            " OR tl9.occupation LIKE '%", p_search, "%'",
            " OR tl7.province LIKE '%", p_search, "%')"
        );
    END IF;

    IF p_search_employer IS NOT NULL AND p_search_employer != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl6.employer LIKE '%", p_search_employer, "%'");
    END IF;

    IF p_search_address IS NOT NULL AND p_search_address != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl6.address LIKE '%", p_search_address, "%'");
    END IF;

    IF p_ddl_occupation IS NOT NULL AND p_ddl_occupation != '' THEN
        SET sql_where = CONCAT(sql_where, " AND FIND_IN_SET(tl9.noc_code, '", p_ddl_occupation, "') > 0");
    END IF;

    IF p_ddl_province IS NOT NULL AND p_ddl_province != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl7.province_slug = '", p_ddl_province, "'");
    END IF;
   
   -- ======================================
    -- MAIN QUERY (Single Scan)
    -- ======================================

    SET @query = CONCAT(
        "SELECT SQL_CALC_FOUND_ROWS
            tl6.employer,
            tl6.address,
            tl6.lmiasponsored,
            tl6.timelinesponsor,
            tl9.occupation AS occupation,
            tl6.employer_slug, 
            tl9.occupation_slug
        ", sql_base, sql_where,
        " GROUP BY tl6.employerid
          ORDER BY 
          CASE 
              WHEN tl10.Year = (SELECT MAX(Year) FROM trax2022_lmiayear) 
               AND tl10.Quarter = (SELECT MAX(Quarter) FROM trax2022_lmiayear 
                                   WHERE Year = (SELECT MAX(Year) FROM trax2022_lmiayear)) 
              THEN 1 ELSE 2 
          END,
          RAND()
          LIMIT ", p_start, ", ", p_length
    );

    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- ======================================
    -- TOTAL COUNT (No second heavy scan)
    -- ======================================
    SELECT FOUND_ROWS() AS total_count;
    
END