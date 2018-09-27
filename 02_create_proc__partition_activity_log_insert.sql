USE `dba_utils`;
DROP procedure IF EXISTS `partition_activity_log_insert`;

DELIMITER $$
USE `dba_utils`$$
CREATE PROCEDURE `partition_activity_log_insert` (object_name VARCHAR(200), execute_datetime DATETIME, start_or_stop CHAR(5))
BEGIN
/*==================================================================================================
Purpose:  Inserts records in the partition_activity_log table to monitor partition times.

Modified:	
Date       Version    Developer      Comments

8/18/2017    1.0      M. Westrell   Original, based on B. Guarnieri and Michael Bourgon code

Parameters:                                                                                      
             object_name: Name of the object that's being logged                                    
             execute_datetime : Datetime of this sproc's execution                                 
             start_or_stop : Is the process completing or stopping                                   
																								    
Usage: CALL `dba_utils`.`partition_activity_log_insert` 'procedure name', 'start'; 
===================================================================================================*/
	DECLARE max_id INT;
	DECLARE error NVARCHAR(100);

-- error handler declaration  
    DECLARE sql_issue CONDITION FOR SQLSTATE '45000';

-- error check 
    IF ( ( object_name IS NULL OR object_name = '') OR ( start_or_stop IS NULL OR start_or_stop = '') ) THEN
			SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The object_name and start_or_stop arguments cannot be NULL or empty strings.  Log record could not be inserted.';
	END IF;
      
-- set value if not passed
	IF (execute_datetime IS NULL) THEN
		SET execute_datetime = CURRENT_TIMESTAMP();
	END IF;

-- insert start entry
	IF (LCASE(start_or_stop) = 'start') THEN
            
            INSERT INTO `partition_activity_log` 
            (
              `proc_name`
            , `start_time` 
            , `start_or_stop` 
            )
            VALUES 
            (
              object_name
			, execute_datetime
            , 'start'
            );
            
	END IF;

-- update an existing entry with the stop datetime and process
	IF (LCASE(start_or_stop) = 'stop') THEN	
    
			SELECT  MAX(`id`)
            INTO max_id
			FROM `partition_activity_log`
			WHERE ( `proc_name` = object_name);

			UPDATE `partition_activity_log`
			SET `end_time` = execute_datetime
            , `start_or_stop` = 'stop'
			WHERE (`proc_name` = object_name)
			AND   (`end_time` IS NULL)
			AND   (`id` = max_id);

	END IF;

END$$

DELIMITER ;