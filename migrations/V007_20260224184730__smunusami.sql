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