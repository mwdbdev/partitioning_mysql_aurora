USE `dba_utils`;
DROP procedure IF EXISTS `partition_columns_check`;

DELIMITER $$
USE `dba_utils`$$
CREATE PROCEDURE `partition_columns_check` (tbl_schema_name VARCHAR(64), tbl_name VARCHAR(64), script_only BIT)
col_chk:BEGIN
/* ==========================================================================================================
Purpose:   Check for the id and insert_datetime columns that are used to partition tables.  Add the column(s)
		   if they are missing.  Then change the primary key to these two columns.  The id column must BEGIN
           first in order for the auto_increment feature to work.
Modified:	 
Date       Version    Developer      Comments

3/2/2018     1.0      M. Westrell    Original - JIRA CIMA-72

Example of call:  CALL `dba_utils`.`partition_columns_check`('ReportData_RTP', 'rpt_cdn_cto_collector_v01');
========================================================================================================== */
	DECLARE row_ct INT;
    DECLARE pk_cols VARCHAR(50);
    DECLARE new_column VARCHAR(200);
    DECLARE pk_clause VARCHAR(100);

-- Select existing PK and make it a unique index if it is something other than id or id,insert_datetime so we 
-- continue to support application querying.  We capture the PK column info here and create at the end of the proc.
	SELECT GROUP_CONCAT(COLUMN_NAME) AS `PK`
    INTO pk_cols
	FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
	WHERE
	  TABLE_SCHEMA = tbl_schema_name
      AND TABLE_NAME = tbl_name
	  AND CONSTRAINT_NAME='PRIMARY';	

	IF (pk_cols = 'id,insert_datetime') THEN
		LEAVE col_chk;
    END IF;
    
-- only drop PK if a primary key exists    
    IF (pk_cols IS NULL) THEN
		SET pk_clause = ' ADD PRIMARY KEY (`id` ASC, `insert_datetime` ASC)';
    ELSE
		SET pk_clause = ' DROP PRIMARY KEY, ADD PRIMARY KEY (`id` ASC, `insert_datetime` ASC)';    
    END IF;

-- initialize variables
	SET new_column = '';
    SET row_ct = 0;
    IF (script_only IS NULL) THEN
		SET script_only = 1;
    END IF;

-- add insert_datetime column if it doesn't exist
	SELECT COUNT(`c`.`ORDINAL_POSITION`)
    INTO row_ct
    FROM `information_schema`.`COLUMNS` `c`
    WHERE (`c`.`TABLE_SCHEMA` = tbl_schema_name)
    AND   (`c`.`TABLE_NAME` = tbl_name)
    AND   ( `c`.`COLUMN_NAME` = 'insert_datetime');

	IF (row_ct = 0) THEN
		SET new_column = ' ADD `insert_datetime` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP';
		SET @step_text = CONCAT('ALTER TABLE `', tbl_schema_name, '`.`', tbl_name, '`', new_column, ';');
        IF (script_only = 1) THEN
			SELECT @step_text;
		ELSE
			PREPARE stmt1 FROM @step_text;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
        END IF;
    END IF;

-- initialize variables
	SET new_column = '';
    SET row_ct = 0;

-- determine if id column exists
	SELECT COUNT(`c`.`ORDINAL_POSITION`)
    INTO row_ct
    FROM `information_schema`.`COLUMNS` `c`
    WHERE (`c`.`TABLE_SCHEMA` = tbl_schema_name)
    AND   (`c`.`TABLE_NAME` = tbl_name)
    AND   ( `c`.`COLUMN_NAME` = 'id');

-- set the complete ALTER TABLE statement
	IF (row_ct = 0) THEN
    -- add id column if it doesn't exist
		SET new_column = ' ADD `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, ';
		SET @step_text = CONCAT('ALTER TABLE `', tbl_schema_name, '`.`', tbl_name, '`', new_column, pk_clause, ';');
	ELSEIF (pk_cols IS NULL) THEN
	-- create PK but there is no PK to drop
		SET @step_text = CONCAT('ALTER TABLE ', tbl_schema_name, '.', tbl_name, pk_clause, ';');        
	ELSE 
    -- change pk
		SET @step_text = CONCAT('ALTER TABLE ', tbl_schema_name, '.', tbl_name, pk_clause, ';');
    END IF;

-- select text or execute it
	IF (script_only = 1) THEN
		SELECT @step_text;
	ELSE
		PREPARE stmt1 FROM @step_text;
		EXECUTE stmt1;
		DEALLOCATE PREPARE stmt1;
	END IF;
	
-- Create unique index. Since UNIQUE INDEX must include all columns in the table's partitioning function, 
-- we wait to create the unique index until the partitioning columns are added.  
	IF (pk_cols IS NOT NULL AND pk_cols NOT IN ('id', 'id,insert_datetime')) THEN
		SET @step_text = CONCAT('CREATE UNIQUE INDEX `ncidx_', REPLACE(pk_cols, ',', '_'), '` ON `', tbl_schema_name, '`.`', tbl_name, '` (', pk_cols, ',id,insert_datetime);');
        IF (script_only = 1) THEN
			SELECT @step_text;
		ELSE
			PREPARE stmt1 FROM @step_text;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
        END IF;        
    END IF;

END$$
DELIMITER ;