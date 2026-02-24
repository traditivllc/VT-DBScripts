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