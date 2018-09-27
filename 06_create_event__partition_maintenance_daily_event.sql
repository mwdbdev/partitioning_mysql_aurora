/*
Purpose:  create a scheduled event that will run the procedures that add and drop partitions.
           
WARNINGS:  Before running: 
1. Verify the two stored procedures that will be called have been created in the database.
2. Adjust the start date and time to whatever the low point of actitiy is for the database and its host server.
*/
#9/12/2017 - Revised event to pass a NULL argument.
DROP event IF EXISTS `partition_maintenance_daily_event`;

DELIMITER $$
USE `dba_utils`$$

CREATE 
EVENT `partition_maintenance_daily_event` 
ON SCHEDULE EVERY 1 DAY STARTS '2018-03-01 01:18:00'
ON COMPLETION PRESERVE ENABLE 
DO BEGIN
-- add partitions
	CALL `dba_utils`.`part_add_future_partition_month`(NULL, NULL);

-- drop partitions
    CALL  `dba_utils`.`part_drop_oldest_partition_month`(NULL, NULL);
END $$

DELIMITER ;