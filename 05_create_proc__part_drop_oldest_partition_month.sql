USE `dba_utils`;
DROP procedure IF EXISTS `part_drop_oldest_partition_month`;

DELIMITER $$
CREATE PROCEDURE `part_drop_oldest_partition_month`(IN `tblSchema` VARCHAR(64), IN `tblName` VARCHAR(64))
BEGIN
/* =====================================================================================================
Purpose:   Switches the data in the oldest month partitions to the "aux" table, drops that same  
           partition in the primary table. and then truncates the aux table.  Note that in MySQL,
           the "aux" table is not partitioned.  As a result, the code is somewhat different than
           the code in the SQL Server [part].[del_partition_left_on_ps_by_parameter] procedure.
Modified:	 
Date       Version    Developer      Comments 

6/4/2018     1.5      M. Westrell	 Added code to drop the aux table just before the code that
                                     creates the aux table.  Reason for this change is that
                                     something in AWS is causing the aux table to be "different" 
                                     from the base table.  Suspect the aux table is being moved when
                                     there is drive auto-growth, but not sure.
4/11/2018    1.4      M. Westrell	 Added join with the information_schema.TABLES WHERE 
                                     (CREATE_OPTIONS = 'partitioned') to filter out tables
                                     that are in the tracking table but are not partitioned.
2/28/2018    1.3      M. Westrell    Changed table schema for logging procedures to dba_utils.  Added
                                     tblSchema argument since the tables can be in other schemas than
                                     dba_utils.
2/1/2018     1.2      M. Westrell    Made four changes.  These queries started returning multiple
                                     rows instead of one on 1/24/2018; cause unknown.
                                     1.  Added "DISTINCT" to query that sets the p2_cutoff_date 
                                     variable value.  Old query started returning two rows.

									 2.  Added "DISTINCT" to query that sets the 
									 p_oldest_retain_ordinal_position variable value.  
									 Old query started returning two rows.
									 									 
									 3.  Added "WHERE (`TABLE_SCHEMA` = tbl_in_progress_schema)" and 
									 "AND (PARTITION_ORDINAL_POSITION = 1)" to the predicate, changed 
									 "`PARTITION_DESCRIPTION` = '0'" to "(from_days(`PARTITION_DESCRIPTION`) 
									 = '0000-00-00')", and added "DISTINCT" to the query that sets the
									 empty_partition_name rec_ct variables.  Old query started returning 
									 four rows.
9/12/2017	 1.1	  M. Westrell	 Add argument to procedure so it can be run for just one table.  
                                     Goal is to make testing the process of dropping partitions to a 
                                     new table easy.
8/22/2017    1.0      M. Westrell    Original, based on code by Michael Bourgon and B. Guarnieri.

Example of call:  CALL `dba_utils`.`part_drop_oldest_partition_month`();
===================================================================================================== */
-- counters
	DECLARE id_counter INT;
    DECLARE max_id INT;
-- partitioned table variables
	DECLARE tbl_in_progress_schema VARCHAR(64);
    DECLARE table_in_process VARCHAR(64);
    DECLARE p_col_name VARCHAR(64);
    DECLARE retention_months INT;
    DECLARE aux_tbl_name VARCHAR(64);
-- partition values    
    DECLARE empty_partition_name VARCHAR(64);
    DECLARE rec_ct INT;
    DECLARE p2_cutoff_date DATE;
-- removal variables
    DECLARE p_oldest_retain_date DATETIME;
    DECLARE p_oldest_retain_ordinal_position INT;
	DECLARE partitions_to_remove TINYINT;
	DECLARE p_cutoff_date DATE;
    DECLARE p_cutoff_string VARCHAR(10);
    DECLARE p_old_name varchar(20);    
-- error handling    
    DECLARE error_msg VARCHAR(2000);

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
	CALL `partition_activity_log_insert`('part_drop_oldest_partition_month', NOW(), 'start');

/*----------------------------------------OUTER LOOP START----------------------------------------*/ 
	WHILE  (id_counter <= max_id) DO

	-- populate variables
		SELECT 	`table_schema`
 		, `table_name`
 		, `p_column_name`
        , `p_retention`
        INTO tbl_in_progress_schema, table_in_process, p_col_name, retention_months
		FROM  `tmp_tbl_list` 
        WHERE (`id` = id_counter);

	-- change retention_months value to a negative number
        SET retention_months = retention_months * (-1);

	-- get minimum partition name and stop process if oldest partition is not set to 0000-00-00
        SELECT  DISTINCT `PARTITION_NAME`
        , COUNT(`PARTITION_NAME`)
        INTO empty_partition_name, rec_ct
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE (`TABLE_SCHEMA` = tbl_in_progress_schema)
        AND   (`TABLE_NAME` = table_in_process)
		AND   (from_days(`PARTITION_DESCRIPTION`) = '0000-00-00')
        AND   (PARTITION_ORDINAL_POSITION = 1);  

	-- Stop processing if the empty partition, which should be the first partition, is missing.  
		IF ( rec_ct <> 1 ) THEN
			SET error_msg = CONCAT('The empty partition in the ', tbl_in_progress_schema, '.', table_in_process, ' is missing.  Old partitions cannot be removed until this is corrected.');
			SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_msg;
		END IF;

	-- get the information from partition 2.
    -- the partition boundary is stored in the PARTITION_DESCRIPTION column as a date serial number
		SELECT DISTINCT FROM_DAYS(`PARTITION_DESCRIPTION`)
        INTO   p2_cutoff_date
        FROM   INFORMATION_SCHEMA.PARTITIONS
        WHERE  (`TABLE_NAME` = table_in_process)
        AND   (`TABLE_SCHEMA` = tbl_in_progress_schema)        
		AND    (`PARTITION_ORDINAL_POSITION` = 2);

	-- Determine what the min partition date to be retained should be.  Calculate what the partition_description should be, based on the current month's partition minus
    -- the retention_months.  Then find the name and ordinal position of that partition, and calculate how many partitions exist that are older than that partition.
		SET p_oldest_retain_date = (SELECT DATE_ADD(LAST_DAY(DATE_ADD(DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY), INTERVAL retention_months MONTH)), INTERVAL 1 DAY));

        SELECT DISTINCT `PARTITION_ORDINAL_POSITION`
        INTO   p_oldest_retain_ordinal_position 
        FROM   INFORMATION_SCHEMA.PARTITIONS
        WHERE  (`TABLE_NAME` = table_in_process) 
        AND   (`TABLE_SCHEMA` = tbl_in_progress_schema)        
        AND    (`PARTITION_DESCRIPTION` = TO_DAYS(p_oldest_retain_date));

	-- error check
		IF (p_oldest_retain_ordinal_position IS NULL) THEN
			SET error_msg = CONCAT('There is no partition that matches the calculated p_oldest_retain_date variable value of ', CAST(p_oldest_retain_date AS CHAR(10)), 'in the ', tbl_in_progress_schema, '.', table_in_process, ' table.  Old partitions cannot be removed at this time.');
			SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_msg;
        END IF;

	-- get the number of partitions to remove
		SET partitions_to_remove = (SELECT p_oldest_retain_ordinal_position - 2);

	-- add activity step record
		SET @step_text = CONCAT('The ', tbl_in_progress_schema, '.', table_in_process, ' table needs ', CAST(partitions_to_remove as CHAR(2)), ' partitions dropped.');

		CALL `dba_utils`.`partition_activity_steps_log_insert` 
		(
		  'drop oldest partition month'
		, tbl_in_progress_schema		
        , table_in_process
		, @step_text
		, NOW()
		); 

        SET @step_text = '';

		IF ( partitions_to_remove > 0 )  THEN        

		-- set name of the aux table
			SET aux_tbl_name = CONCAT(table_in_process, '_aux');

		-- drop the aux table - v1.5
			IF ((SELECT COUNT(`TABLE_NAME`) FROM `INFORMATION_SCHEMA`.`TABLES` WHERE (`TABLE_SCHEMA` = tbl_in_progress_schema AND `TABLE_NAME` = aux_tbl_name)) = 1) THEN
				SET @step_text_1 = CONCAT('DROP TABLE `', tbl_in_progress_schema, '`.`', aux_tbl_name, '`;');

			-- log the activity step
				CALL `dba_utils`.`partition_activity_steps_log_insert` 
				(
				  'drop oldest partition month'
		        , tbl_in_progress_schema				
                , table_in_process
				, @step_text_1
				, NOW()
				); 

			-- drop the aux table
				PREPARE stmt1 FROM @step_text_1;
				EXECUTE stmt1;
				DEALLOCATE PREPARE stmt1;            

			END IF;
            
		-- create the aux table if it doesn't exist.  Note the table creation and removal of partitioning have to be in two
		-- separate statement executions in order to run successfully.
			IF ((SELECT COUNT(`TABLE_NAME`) FROM `INFORMATION_SCHEMA`.`TABLES` WHERE (`TABLE_SCHEMA` = tbl_in_progress_schema AND `TABLE_NAME` = aux_tbl_name)) = 0) THEN

			-- generate statements to create the aux table and remove partitioning
				SET @step_text_1 = CONCAT('CREATE TABLE `', tbl_in_progress_schema, '`.`', aux_tbl_name, '` LIKE `', tbl_in_progress_schema, '`.`', table_in_process, '`;');
				SET @step_text_2 = CONCAT('ALTER TABLE `', tbl_in_progress_schema, '`.`', aux_tbl_name, '` REMOVE PARTITIONING;');

			-- log the activity step
				CALL `dba_utils`.`partition_activity_steps_log_insert` 
				(
				  'drop oldest partition month'
		        , tbl_in_progress_schema				
                , table_in_process
				, @step_text_1
				, NOW()
				); 

			-- create the aux table
				PREPARE stmt1 FROM @step_text_1;
				EXECUTE stmt1;
				DEALLOCATE PREPARE stmt1;            

			ELSE

			-- Truncate an existing aux table.  If processed failed on the TRUNCATE TABLE step, there could still be records in the aux table.
			-- Removing them here will prevent them from being passed back into the base table partition when the EXCHANGE command executes.
				SET @step_text_2 = CONCAT('TRUNCATE TABLE `', tbl_in_progress_schema, '`.`', aux_tbl_name, '`;');

			END IF;

		-- log the activity step
			CALL `dba_utils`.`partition_activity_steps_log_insert` 
			(
			  'drop oldest partition month'
			, tbl_in_progress_schema
            , table_in_process
			, @step_text_2
			, NOW()
			); 

		-- remove the partitions OR truncate existing aux table
			PREPARE stmt1 FROM @step_text_2;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
			
			SET @step_text = '';
        
        END IF;

/*----------------------------------------INNER LOOP START----------------------------------------*/ 
		WHILE ( partitions_to_remove > 0 ) DO

		-- get the name of the partition to remove
			SET p_cutoff_date = DATE_ADD(p2_cutoff_date, INTERVAL -1 MONTH) ;
            SET p_cutoff_string = CAST(p_cutoff_date AS CHAR(10));
			SET p_old_name = CONCAT('p_', SUBSTRING(REPLACE(p_cutoff_string, '-', ''), 1, 6));

		-- generate the statements to exchange the oldest partition with the aux table, truncate the aux table, and drop the partition
			SET @step_text_1 = CONCAT('ALTER TABLE `', tbl_in_progress_schema, '`.`', table_in_process, '` EXCHANGE PARTITION ', p_old_name, ' WITH TABLE `', tbl_in_progress_schema, '`.`', aux_tbl_name, '`;');
            SET @step_text_2 = CONCAT('TRUNCATE TABLE `', tbl_in_progress_schema, '`.`', aux_tbl_name, '`;');
            SET @step_text_3 = CONCAT('ALTER TABLE `', tbl_in_progress_schema, '`.`', table_in_process, '` DROP PARTITION ', p_old_name, ';');

		-- log step 1
			CALL `dba_utils`.`partition_activity_steps_log_insert` 
			(
			  'drop oldest partition month'
			, tbl_in_progress_schema
            , table_in_process
			, @step_text_1
			, NOW()
			);         

		-- log step 2
			CALL `dba_utils`.`partition_activity_steps_log_insert` 
			(
			  'drop oldest partition month'
			, tbl_in_progress_schema
            , table_in_process
			, @step_text_2
			, NOW()
			);         

		-- log step 3
			CALL `dba_utils`.`partition_activity_steps_log_insert` 
			(
			  'drop oldest partition month'
			, tbl_in_progress_schema
            , table_in_process
			, @step_text_3
			, NOW()
			);         

		-- execute step 1
			PREPARE stmt1 FROM @step_text_1;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;   

		-- execute step 2
			PREPARE stmt1 FROM @step_text_2;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;  

		-- execute step 3
			PREPARE stmt1 FROM @step_text_3;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;              

		-- increment variables 
			SET partitions_to_remove = partitions_to_remove - 1;

		-- get the next partition
			IF (partitions_to_remove > 0) THEN
            
			-- get the information from partition 2.
			-- the partition boundary is stored in the PARTITION_DESCRIPTION column as a date serial number
				SELECT  FROM_DAYS(`PARTITION_DESCRIPTION`)
				INTO p2_cutoff_date
				FROM INFORMATION_SCHEMA.PARTITIONS
				WHERE (`TABLE_NAME` = table_in_process) 
				AND   (`TABLE_SCHEMA` = tbl_in_progress_schema) 
				AND   (`PARTITION_ORDINAL_POSITION` = 2);

			END IF;

		END WHILE;

/*----------------------------------------INNER LOOP END----------------------------------------*/ 

        SET id_counter = id_counter + 1;

	END WHILE;

/*----------------------------------------OUTER LOOP END----------------------------------------*/

-- insert log entry
	CALL `partition_activity_log_insert`('part_drop_oldest_partition_month', NOW(), 'stop');

END$$
DELIMITER ;
