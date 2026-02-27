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