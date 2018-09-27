USE `dba_utils`;
DROP procedure IF EXISTS `partition_activity_steps_log_insert`;

DELIMITER $$
USE `dba_utils`$$
CREATE PROCEDURE `partition_activity_steps_log_insert` 
(group_action VARCHAR(50), table_schema VARCHAR(64), tbl_name VARCHAR(64), step_last_run VARCHAR(2000), execute_datetime DATETIME)
BEGIN
/*=================================================================================================================================
Purpose:  Inserts records in the partition_activity_steps_log table to monitor partition step times.

Modified:	
Date       Version    Developer      Comments

2/28/2017    1.1      M. Westrell    Changed argument name from "database_name" to "table_schema" to match MySQL nomenclature.
8/18/2017    1.0      M. Westrell    Original, based on B. Guarnieri and Michael Bourgon code

Parameters:
             group_action:    identifies the activity the step is related to                                                                                      
             table_schema:    name of the parent database                                    
             tbl_name:        the table being changed
             step_last_run:   the text of the step to be run
             execute_datetime the datetime this procedure was called
																								    
Usage: CALL `partition_test`.`partition_activity_steps_log_insert` 'group_action', 'table_schema', 'table_name', 'step text'; 
=================================================================================================================================*/
-- error handler declaration  
    DECLARE sql_issue CONDITION FOR SQLSTATE '45000';

-- error check 
    IF ( ( table_schema IS NULL OR table_schema = '') OR ( group_action IS NULL OR group_action = '')  
    OR ( tbl_name IS NULL OR tbl_name = '') OR ( step_last_run IS NULL OR step_last_run = '') ) THEN
		SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'The arguments for this procedure cannot be NULL or empty strings.  Log record could not be inserted.';
	END IF;

-- set value if not passed
	IF (execute_datetime IS NULL) THEN
		SET execute_datetime = CURRENT_TIMESTAMP();
	END IF;

-- insert record
	INSERT INTO `partition_activity_steps_log`
	(
	  `group_action`
	, `table_schema`
    , `table_name`
    , `step_last_run`
    , `last_activity`
	)
	VALUES
    (
      group_action
	, table_schema
    , tbl_name
    , step_last_run
    , execute_datetime
    );

END$$
DELIMITER ;