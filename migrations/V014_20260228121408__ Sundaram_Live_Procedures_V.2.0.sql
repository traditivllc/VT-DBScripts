drop procedure if exists PR_trax2022_get_lmia_employer_details;

CREATE PROCEDURE PR_trax2022_get_lmia_employer_details(
    IN p_employer_slug VARCHAR(500)
)
BEGIN
    DECLARE sql_base TEXT;
    DECLARE sql_where TEXT DEFAULT ' WHERE 1=1 ';
 
    SET sql_base = 
        " FROM trax2022_lmiaemployerapprovals tl 
          INNER JOIN trax2022_lmiaemployer tl6 ON tl6.employerid = tl.employerid 
          INNER JOIN trax2022_lmiaemployeroccupations tl3 ON tl.leoid = tl3.leoid AND tl3.employerid = tl6.employerid 
          INNER JOIN trax2022_lmiaemployerstreams tl4 ON tl.lesid = tl4.lesid AND tl4.employerid = tl6.employerid 
          INNER JOIN trax2022_lmiaemployerprovences tl5 ON tl.lepid = tl5.lepid AND tl5.employerid = tl6.employerid 
          INNER JOIN trax2022_lmiaprovince tl7 ON tl7.provinceid = tl5.provinceid 
          INNER JOIN trax2022_lmiaprogramstream tl8 ON tl8.streamid = tl4.streamid 
          INNER JOIN trax2022_lmiaoccupation tl9 ON tl9.occupationid = tl3.occupationid 
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
            tl10.quarter AS 'quarters',
            tl.approved2,
            tl6.islmia,
            CASE 
                WHEN tl10.quarter = 'Quarter1' THEN 'January to March'
                WHEN tl10.quarter = 'Quarter2' THEN 'April to June'
                WHEN tl10.quarter = 'Quarter3' THEN 'July to September'
                ELSE 'October to December' 
            END AS 'quarter',
            tl6.timelinesponsor, 
            CONCAT(tl9.occupation, '|', tl7.province) AS occupation_province,
            (SELECT noc_description 
             FROM trax2022_lmia_noc_update_description nocupdate 
             WHERE noc_code COLLATE utf8mb4_unicode_520_ci = tl9.noc_code COLLATE utf8mb4_unicode_520_ci 
               AND tl8.stream COLLATE utf8mb4_unicode_520_ci = nocupdate.stream COLLATE utf8mb4_unicode_520_ci 
               AND tl7.province COLLATE utf8mb4_unicode_520_ci = nocupdate.province COLLATE utf8mb4_unicode_520_ci) AS noc_description ",
        sql_base, sql_where
    );
 
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;      
   
end;

