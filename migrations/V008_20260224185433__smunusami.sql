CREATE PROCEDURE `trax2022_get_lmia_employer_details`(
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