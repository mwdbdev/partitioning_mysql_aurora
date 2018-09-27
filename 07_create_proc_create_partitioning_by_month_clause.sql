USE `dba_utils`;
DROP procedure IF EXISTS `partitioning_by_month_clause_create`;
DELIMITER $$
USE `dba_utils`$$
CREATE PROCEDURE `partitioning_by_month_clause_create`(schema_name VARCHAR(64), tbl_name VARCHAR(64), partition_months_number INT SIGNED, script_only BIT)
BEGIN
/* =====================================================================================================
Purpose:   Generates the script to partition a table based on the name of the table.
Modified:	 
Date       Version    Developer      Comments 

3/2/2018     1.3      M. Westrell    Added script_only argument and modified procedure so it can
                                     either return the script (script_only = 1) or execute it
                                     (script_only = 0).  If a NULL is passed, the value 
                                     defaults to 1.
2/28/2018    1.2      M. Westrell    Added schema_name to procedure call arguments.  Now that the
                                     partitioning objects are all in the dba_utils schema and don't 
                                     need to be in every database, the schema name is needed to define
                                     which table to create the partition script for.
9/12/2017	 1.1	  M. Westrell	 Add argument to procedure so it can be run for just one table.  
                                     Goal is to make testing the process of dropping partitions to a 
                                     new table easy.
8/22/2017    1.0      M. Westrell    Original, based on code by Michael Bourgon and B. Guarnieri.

Example of call:  CALL `dba_utils`.`partitioning_by_month_clause_create`('schema name', 'table name', 13);
===================================================================================================== */
DECLARE date_today DATE DEFAULT CURRENT_DATE();
DECLARE partition_month DATE;
DECLARE last_day_current DATE;
DECLARE p_cutoff_date DATE;
DECLARE p_date VARCHAR(12) ;
DECLARE p_name CHAR(8) DEFAULT '';
DECLARE v_min INT UNSIGNED DEFAULT 0;
#DECLARE partition_months_number INT SIGNED DEFAULT 12;
DECLARE loop_num INT SIGNED;
DECLARE p_stmt VARCHAR(200) DEFAULT '';
DECLARE partition_clause VARCHAR(2000) DEFAULT '';

#SELECT partition_months_number, v_min;

-- find the last day of the current month
SET last_day_current = LAST_DAY(CURRENT_DATE());

-- add p_empty partition at start of partitions
SET partition_clause = CONCAT('( ', '\n', '  PARTITION p_empty  VALUES LESS THAN (0) ', '\n');

WHILE (partition_months_number >= v_min) DO

-- set the loop number for dateadd calculations
     SET loop_num = partition_months_number * -1;
-- set the partition variable values
	 SET p_cutoff_date = DATE_ADD(DATE_ADD(last_day_current, INTERVAL 1 DAY), INTERVAL loop_num MONTH);
     SET p_date = DATE_FORMAT(DATE_ADD(last_day_current, INTERVAL loop_num MONTH), '%Y%m');
     SET p_name = CONCAT('p_', p_date);

-- concatenate variables into the next line in the partition clause
     SET p_stmt = CONCAT('PARTITION ', p_name, ' VALUES LESS THAN (TO_DAYS(''', CAST(p_cutoff_date AS CHAR(10)),' 00:00:00''', '))', '\n') ;

-- concatentate the next part of the clause
     SET partition_clause = CONCAT(partition_clause, ', ', p_stmt);

-- reset counter
    set partition_months_number = partition_months_number - 1;

  END WHILE;

-- add closing statement
	SET @step_text = CONCAT('ALTER TABLE `', schema_name, '`.`', tbl_name, '`', '\n', 'PARTITION BY RANGE( TO_DAYS(insert_datetime) ) ', '\n', partition_clause, ', ', 'PARTITION p_future VALUES LESS THAN MAXVALUE', '\n', ' );');
-- print the command or execute it
	IF (script_only = 1) THEN
		SELECT @step_text;
	ELSE
		PREPARE stmt1 FROM @step_text;
		EXECUTE stmt1;
		DEALLOCATE PREPARE stmt1;
	END IF;
  
END$$
DELIMITER ;
