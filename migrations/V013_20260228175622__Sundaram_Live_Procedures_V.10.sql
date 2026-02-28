DROP PROCEDURE IF EXISTS trax2022_get_all_lmia_employers_data;

CREATE PROCEDURE trax2022_get_all_lmia_employers_data()
BEGIN

   DECLARE v_max_year INT;
    DECLARE v_max_quarter VARCHAR(20);

    -- Get max year
    SELECT MAX(Year) INTO v_max_year 
    FROM trax2022_lmiayear;

    -- Get latest quarter (based on numeric value inside QuarterX)
    SELECT Quarter 
    INTO v_max_quarter
    FROM trax2022_lmiayear
    WHERE Year = v_max_year
    ORDER BY CAST(REPLACE(Quarter, 'Quarter', '') AS UNSIGNED) DESC
    LIMIT 1;

    SELECT 
        tl.employerid,
        tl6.employer,
        tl6.address,
        tl6.lmiasponsored,
        tl6.timelinesponsor,
        tl7.province,
        tl8.stream,
        tl9.occupation,
        tl9.noc_code,
        tl6.employer_slug, 
        tl7.province_slug,
        tl8.stream_slug,
        tl9.occupation_slug,
        tl10.`year`,
        tl10.quarter 
    FROM trax2022_lmiaemployerapprovals tl 
    INNER JOIN trax2022_lmiaemployer tl6 
        ON tl6.employerid = tl.employerid 
    INNER JOIN trax2022_lmiaemployeroccupations tl3 
        ON tl.leoid = tl3.leoid 
        AND tl3.employerid = tl6.employerid 
    INNER JOIN trax2022_lmiaemployerstreams tl4 
        ON tl.lesid = tl4.lesid 
        AND tl4.employerid = tl6.employerid 
    INNER JOIN trax2022_lmiaemployerprovences tl5 
        ON tl.lepid = tl5.lepid 
        AND tl5.employerid = tl6.employerid 
    INNER JOIN trax2022_lmiaprovince tl7 
        ON tl7.provinceid = tl5.provinceid 
    INNER JOIN trax2022_lmiaprogramstream tl8 
        ON tl8.streamid = tl4.streamid 
    INNER JOIN trax2022_lmiaoccupation tl9 
        ON tl9.occupationid = tl3.occupationid 
    INNER JOIN trax2022_lmiayear tl10 
        ON tl10.yearid = tl.yearid

    ORDER BY 
        CASE 
            WHEN tl10.Year = v_max_year 
             AND tl10.Quarter = v_max_quarter
            THEN 1 
            ELSE 2 
        END;

END

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_lmia_employers;

CREATE PROCEDURE `trax2022_get_lmia_employers`(
    IN p_start INT,
    IN p_length INT,
    IN p_search VARCHAR(200),
    IN p_order_column VARCHAR(50),
    IN p_order_dir VARCHAR(4),
    IN p_ddl_occupation VARCHAR(5000),
    IN p_ddl_province VARCHAR(5000),
    IN p_search_employer VARCHAR(500),
    IN p_search_stream VARCHAR(500),
    IN p_search_province VARCHAR(5000),
    IN p_search_occupation VARCHAR(500)
)
BEGIN
    DECLARE sql_base TEXT;
    DECLARE sql_where TEXT DEFAULT ' WHERE 1=1 ';
    DECLARE count_sql TEXT;
   
   -- ✅ Escape single quotes to prevent SQL break
    SET p_search = REPLACE(p_search,"'","''");
    SET p_search_employer = REPLACE(p_search_employer,"'","''");
    SET p_search_stream = REPLACE(p_search_stream,"'","''");
    SET p_search_province = REPLACE(p_search_province,"'","''");
    SET p_search_occupation = REPLACE(p_search_occupation,"'","''");
    SET p_ddl_occupation = REPLACE(p_ddl_occupation,"'","''");
    SET p_ddl_province = REPLACE(p_ddl_province,"'","''");
   
   SET sql_base = 
        " from trax2022_lmiaemployerapprovals tl 
inner join trax2022_lmiaemployer tl6 on tl6.employerid = tl.employerid 
inner join trax2022_lmiaemployeroccupations tl3 on tl.leoid = tl3.leoid and tl3.employerid = tl6.employerid 
inner join trax2022_lmiaemployerstreams tl4 on tl.lesid = tl4.lesid and tl4.employerid = tl6.employerid 
inner join trax2022_lmiaemployerprovences tl5 on tl.lepid = tl5.lepid and tl5.employerid = tl6.employerid 
inner join trax2022_lmiaprovince tl7 on tl7.provinceid = tl5.provinceid 
inner join trax2022_lmiaprogramstream tl8 on tl8.streamid  = tl4.streamid 
inner join trax2022_lmiaoccupation tl9 on tl9.occupationid = tl3.occupationid 
INNER JOIN trax2022_lmiayear tl10 ON tl10.yearid = tl.yearid ";
         
    
    IF p_search IS NOT NULL AND p_search != '' THEN
        SET sql_where = CONCAT(sql_where,
            " AND (tl6.employer LIKE '%", p_search, "%'",
            " OR tl8.stream LIKE '%", p_search, "%'",
            " OR tl9.noc_code LIKE '%", p_search, "%'",
            " OR tl9.occupation LIKE '%", p_search, "%'",
            " OR tl7.province LIKE '%", p_search, "%')"
        );
    END IF;

    
    IF p_search_employer IS NOT NULL AND p_search_employer != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl6.employer LIKE '%", p_search_employer, "%'");
    END IF;

    IF p_search_stream IS NOT NULL AND p_search_stream != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl8.stream LIKE '%", p_search_stream, "%'");
    END IF;

    IF p_search_province IS NOT NULL AND p_search_province != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl7.province LIKE '%", p_search_province, "%'");
    END IF;
   
   	IF p_search_occupation IS NOT NULL AND p_search_occupation != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl9.occupation LIKE '%", p_search_occupation, "%'");
    END IF;

    IF p_ddl_occupation IS NOT NULL AND p_ddl_occupation != '' THEN
        SET sql_where = CONCAT(sql_where, " AND FIND_IN_SET(tl9.noc_code, '", p_ddl_occupation, "') > 0");

    END IF;

    IF p_ddl_province IS NOT NULL AND p_ddl_province != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl7.province_slug = '", p_ddl_province, "'");
    END IF;
   
   -- ✅ Total count based on GROUP BY
    SET @count_sql = CONCAT(
        "SELECT COUNT(*) AS total_count FROM (",
        "SELECT tl6.employer, tl8.stream, tl9.occupation, tl7.province ",
        sql_base,
        sql_where,
        " GROUP BY tl6.employer, tl8.stream, tl9.occupation, tl7.province",
        ") AS grouped_table"
    );
    PREPARE stmt1 FROM @count_sql;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;
    
   -- Main data query
    SET @query = CONCAT(
        "SELECT 
           	tl6.employer,
            tl6.address,
            tl6.lmiasponsored,
            tl6.timelinesponsor,
            tl7.province AS province,
            tl8.stream AS stream,
            tl9.occupation AS occupation,
			tl6.employer_slug, 
			tl7.province_slug,
			tl8.stream_slug,
			tl9.occupation_slug	",
            sql_base, sql_where,
           " GROUP BY tl6.employer, tl8.stream, tl9.occupation, tl7.province"
           " ORDER BY 
            CASE 
                WHEN tl10.Year = (SELECT MAX(Year) FROM trax2022_lmiayear) 
                 AND tl10.Quarter = (SELECT MAX(Quarter) FROM trax2022_lmiayear 
                                     WHERE Year = (SELECT MAX(Year) FROM trax2022_lmiayear)) 
                THEN 1 ELSE 2 
            END,
            RAND() ",

        " LIMIT ", p_start, ", ", p_length
    );

    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
       
    
end

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_lmia_employers_list;

CREATE PROCEDURE `trax2022_get_lmia_employers_list`(
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
BEGIN
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
    -- TOTAL EMPLOYER COUNT
    -- ======================================
    SET @count_sql = CONCAT(
        "SELECT COUNT(DISTINCT tl6.employerid) AS total_count ",
        sql_base, sql_where
    );

    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;

    -- ======================================
    -- MAIN DATA QUERY
    -- ======================================
    SET @query = CONCAT(
        "SELECT 
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

END

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_lmia_employers_list_updated;

CREATE PROCEDURE `trax2022_get_lmia_employers_list_updated`(
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

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_lmia_employer_details;

CREATE `trax2022_get_lmia_employer_details`(
    IN p_employer_slug VARCHAR(500)
)
BEGIN
    DECLARE sql_base TEXT;
    DECLARE sql_where TEXT DEFAULT ' WHERE 1=1 ';

    SET sql_base = 
        " from trax2022_lmiaemployerapprovals tl 
inner join trax2022_lmiaemployer tl6 on tl6.employerid = tl.employerid 
inner join trax2022_lmiaemployeroccupations tl3 on tl.leoid = tl3.leoid and tl3.employerid = tl6.employerid 
inner join trax2022_lmiaemployerstreams tl4 on tl.lesid = tl4.lesid and tl4.employerid = tl6.employerid 
inner join trax2022_lmiaemployerprovences tl5 on tl.lepid = tl5.lepid and tl5.employerid = tl6.employerid 
inner join trax2022_lmiaprovince tl7 on tl7.provinceid = tl5.provinceid 
inner join trax2022_lmiaprogramstream tl8 on tl8.streamid  = tl4.streamid 
inner join trax2022_lmiaoccupation tl9 on tl9.occupationid = tl3.occupationid 
INNER JOIN trax2022_lmiayear tl10 ON tl10.yearid = tl.yearid ";
        
    
    IF p_employer_slug IS NOT NULL AND p_employer_slug != '' THEN
        SET sql_where = CONCAT(sql_where, " AND tl6.employer_slug = '", p_employer_slug, "'");
    END IF;
         
    SET @query = CONCAT(
        "SELECT 
            tl7.province,
            tl8.stream,
            tl6.employer,
            tl6.address,
            tl9.occupation,
			tl6.lmiasponsored,
			tl10.year,
			tl10.quarter as 'quarters',
			tl.approved2,
			tl6.islmia,
			case when tl10.quarter = 'Quarter1' then 'January to March'
			when tl10.quarter = 'Quarter2' then 'April to June'
			when tl10.quarter = 'Quarter3' then 'July to September'
			else 'October to December' end as 'quarter',
			tl6.timelinesponsor, 
			CONCAT(tl9.occupation, '|', tl7.province) AS occupation_province,
			(select noc_description from trax2022_lmia_noc_update_description nocupdate where noc_code COLLATE utf8mb4_unicode_520_ci = tl9.noc_code COLLATE utf8mb4_unicode_520_ci and tl8.stream COLLATE utf8mb4_unicode_520_ci = nocupdate.stream COLLATE utf8mb4_unicode_520_ci and tl7.province COLLATE utf8mb4_unicode_520_ci = nocupdate.province COLLATE utf8mb4_unicode_520_ci) as noc_description ",
            sql_base, sql_where
    );

    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
       
    
end

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_lmia_occupation;

CREATE PROCEDURE `trax2022_get_lmia_occupation`(
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
        " FROM trax2022_lmiaoccupation tl ";
    
    IF p_search IS NOT NULL AND p_search != '' THEN
        SET sql_where = CONCAT(sql_where,
            " AND (tl.occupation LIKE '%", p_search, "%')");
    END IF;
    
    SET @query = CONCAT(
        "SELECT 
            tl.occupationid,
            tl.occupation,
            tl.isactive,
			tl.occupation_slug,
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

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_lmia_province;

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

--------------------------------------------------

DROP PROCEDURE IF EXISTS trax2022_get_noc_update_description;

CREATE PROCEDURE `trax2022_get_noc_update_description`(
    IN p_start INT,
    IN p_count INT
)
begin
	
-- Calculate offset for LIMIT
SET @offset = p_start - 1;

-- Build query with correct LIMIT offset
SET @query = CONCAT(
    "SELECT nocupdate.id, nocupdate.noc_code, nocupdate.occupation, nocupdate.province, nocupdate.stream, nocupdate.noc_teer
     FROM trax2022_lmia_noc_update_description nocupdate
     WHERE is_updated = 0 
     ORDER BY id ASC
     LIMIT ", @offset, ", ", p_count
);

-- Execute dynamic query
PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END

--------------------------------------------------