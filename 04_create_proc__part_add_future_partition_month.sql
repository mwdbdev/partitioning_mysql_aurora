USE `dba_utils`;
DROP procedure IF EXISTS `part_add_future_partition_month`;

DELIMITER $$
USE `dba_utils`$$
CREATE PROCEDURE `part_add_future_partition_month`(IN `tblSchema` VARCHAR(64), IN `tblName` VARCHAR(64))
BEGIN 
/* =====================================================================================================
Purpose:   Adds partitions to partition schemes and partition functions based 
           on days.  Procdure finds the current partition and splits it.
Modified:	 
Date       Version    Developer      Comments 

4/11/2018    1.4      M. Westrell	 Added join with the information_schema.TABLES WHERE 
                                     (CREATE_OPTIONS = 'partitioned') to filter out tables
                                     that are in the tracking table but are not partitioned.
2/28/2018    1.3      M. Westrell    Changed table schema for logging procedures to dba_utils.  Added
                                     tblSchema argument since the tables can be in other schemas than
                                     dba_utils.  Added new_flag parameter so that inserts are not made
                                     to the partition_actitity_log when it is partitioned.
2/1/2018     1.2      M. Westrell    Made one change.  These queries started returning multiple
                                     rows instead of one on 1/24/2018; cause unknown.
									 1.  Added "DISTINCT" to query that gets max partition name 
                                         and checks for rows.
9/12/2017	 1.1	  M. Westrell	 Add argument to procedure so it can be run for just one table.  
                                     Goal is to make testing the process of adding partitions to a 
                                     new table easy.
8/31/2017    1.0      M. Westrell    Original, based on code by Michael Bourgon and Brian Guarnieri.

Example of call:  CALL `dba_utils`.`part_add_future_partition_month`();
===================================================================================================== */
 
	DECLARE id_counter INT;
    DECLARE max_id INT;
    DECLARE max_from_days DATE;
    DECLARE max_future_month DATETIME;
    DECLARE p_cutoff_date DATE;
    DECLARE p_cutoff_string VARCHAR(10);
    DECLARE p_new_name varchar(20);
    DECLARE sqlCmd TEXT;
    DECLARE sqlCmd2 VARCHAR(200);
    DECLARE db_name VARCHAR(64);
    DECLARE tbl_in_progress_schema VARCHAR(64);
    DECLARE table_in_process VARCHAR(64);
    DECLARE p_col_name VARCHAR(64);
    DECLARE rec_ct INT;
    DECLARE months_to_add TINYINT;
    DECLARE invalid_records_table_name VARCHAR(64);
    DECLARE max_partition_name VARCHAR(64);
    DECLARE step_text VARCHAR(2000);
    DECLARE LF CHAR(2);

-- error handler declaration  
    DECLARE sql_issue CONDITION FOR SQLSTATE '45000';
  
  -- ceate temproary table and populate with table list
    DROP TABLE IF EXISTS `tmp_tbl_list`;
  
	CREATE TEMPORARY TABLE `tmp_tbl_list`
    (
	  `id` int NOT NULL AUTO_INCREMENT
	, `table_schema` VARCHAR(64) NOT NULL
	, `table_name` VARCHAR(64) NOT NULL
    , `p_column_name` VARCHAR(64) NOT NULL
    , `p_retention` INT NOT NULL
    , PRIMARY KEY (`id` ASC)
    );

    INSERT INTO `tmp_tbl_list`
    (
	  `table_schema`
	, `table_name`
    , `p_column_name`
    , `p_retention`  
    )
	SELECT `p`.`table_schema`
  		 , `p`.`table_name`
		 , `p`.`p_column_name`
		 , `p`.`p_retention` 
    FROM   `dba_utils`.`partition_tbl_list` `p`
    INNER JOIN `information_schema`.`TABLES` `t` 
    ON `p`.`table_schema` = `t`.`TABLE_SCHEMA` AND `p`.`table_name` = `t`.`TABLE_NAME`
    WHERE (`t`.`CREATE_OPTIONS` = 'partitioned')
    AND   (`p`.`is_active` = 1 AND `p`.`p_frequency` = 'month')
    AND   ( 1 = (CASE WHEN tblName IS NULL THEN 1 
               WHEN `p`.`table_name` = tblName AND `p`.`table_schema` = tblSchema THEN 1 
               ELSE 0 
               END)
		  );
-- error check 
    IF ((SELECT COUNT(`table_name`) FROM `tmp_tbl_list` ) = 0) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No active records found in the `partition_tbl_list` table.';
	END IF;

-- set minimum and maximum counters for iteration			 
	SET id_counter = (SELECT MIN(`id`) FROM `tmp_tbl_list`) ; 
	SET max_id = (SELECT MAX(`id`) FROM `tmp_tbl_list`);

-- insert log entry
	CALL `partition_activity_log_insert`('part_add_future_partition_month', NOW(), 'start');

/*----------------------------------------OUTER LOOP START----------------------------------------*/
	WHILE (id_counter <= max_id) DO

	-- populate variables
		SELECT 	`table_schema`
 		, `table_name`
        , `p_column_name`
        INTO tbl_in_progress_schema, table_in_process, p_col_name
		FROM  `tmp_tbl_list` 
        WHERE (`id` = id_counter);
 
	-- get max partition name and check for rows
		SELECT  DISTINCT PARTITION_NAME
		, TABLE_ROWS
        INTO max_partition_name, rec_ct
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE (`TABLE_NAME` = table_in_process)
        AND   (`TABLE_SCHEMA` = tbl_in_progress_schema)
		AND   (`PARTITION_DESCRIPTION` = 'MAXVALUE');

	-- Stop processing if there are table rows in the future partition.  This should never happen
    -- if insert_datetime is the partitioned column and partitions are added before the current
    -- date is greater than the max partition with a LESS THAN date.  
		IF ( rec_ct > 0 ) THEN
			SET step_text = CONCAT('The max partition in the ', tbl_in_progress_schema, '.', table_in_process, ' has records in it.  New partitions cannot be added until this is corrected.');
			SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = step_text;
		END IF;

	-- find the most recent month partition; exclude the partition set to 'max_value' .
    -- the partition boundary is stored in the PARTITION_DESCRIPTION column as a date serial number
        SELECT MAX(FROM_DAYS(`PARTITION_DESCRIPTION`))
        INTO  max_from_days
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE (`TABLE_NAME` = table_in_process)
        AND   (`TABLE_SCHEMA` = tbl_in_progress_schema)
		AND   (`PARTITION_DESCRIPTION` <> 'MAXVALUE');

	-- determine what the max partition month should be - 3 months greater than the current month
        SET max_future_month = (SELECT  DATE_ADD(DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY), INTERVAL 3 MONTH));
        SET months_to_add = (SELECT TIMESTAMPDIFF(MONTH, max_from_days, max_future_month));

	-- add activity step record
		SET @step_text = CONCAT('The ', tbl_in_progress_schema, '.', table_in_process, ' table needs ', CAST(months_to_add as CHAR(2)), ' partitions added.');

		CALL `dba_utils`.`partition_activity_steps_log_insert` 
		(
		  'add future partition month'
		, tbl_in_progress_schema
		, table_in_process
		, @step_text
		, NOW()
		); 
       
        SET @step_text = '';
 
/*----------------------------------------INNER LOOP START----------------------------------------*/ 
		WHILE ( months_to_add > 0 ) DO

#SELECT max_from_days; 

			SET p_new_name = CONCAT('p_', SUBSTRING(REPLACE(CAST(max_from_days AS CHAR(10)), '-', ''), 1, 6));
			SET p_cutoff_date = DATE_ADD(max_from_days, INTERVAL 1 MONTH) ;
			SET p_cutoff_string = CAST(p_cutoff_date AS CHAR(10));

		-- insert log entry so there is a record the table was checked even if no partitions need to be added
			SET @step_text = CONCAT('ALTER TABLE `', tbl_in_progress_schema, '`.`', table_in_process, '` REORGANIZE PARTITION ', max_partition_name, ' INTO (PARTITION ', p_new_name, ' VALUES LESS THAN (TO_DAYS(''', p_cutoff_string, ' 00:00:00'') ), PARTITION ', max_partition_name, ' VALUES LESS THAN MAXVALUE);' );

#SELECT @step_text;

			CALL `dba_utils`.`partition_activity_steps_log_insert` 
			(
              'add future partition month'
			, tbl_in_progress_schema
            , table_in_process
			, @step_text
			, current_timestamp()
			); 

        -- add months
			PREPARE stmt1 FROM @step_text;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;			

		-- increment variables       
			SELECT MAX(FROM_DAYS(`PARTITION_DESCRIPTION`))
			INTO  max_from_days
			FROM INFORMATION_SCHEMA.PARTITIONS
			WHERE (`TABLE_NAME` = table_in_process)
            AND   (`TABLE_SCHEMA` = tbl_in_progress_schema)
			AND   (`PARTITION_DESCRIPTION` <> 'MAXVALUE');
            
            SET months_to_add = months_to_add - 1;

		END WHILE;

/*----------------------------------------INNER LOOP END----------------------------------------*/ 

        SET id_counter = id_counter + 1;

	END WHILE;

/*----------------------------------------OUTER LOOP END----------------------------------------*/    

-- insert log entry
	CALL `partition_activity_log_insert`('part_add_future_partition_month', NOW(), 'stop');
    
END$$
DELIMITER ;
