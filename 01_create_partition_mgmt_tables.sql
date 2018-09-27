/*
Purpose:  create the partition management tables and insert 
records into the tracking table for the partition management tables.

Created:  8/1/2017
Author:   M. Westrell
WARNINGS: Before running, make the following updates.
1. Create the dba_utils schema if it doesn't exist.  This is the database that will host the partition management objects.
2. Update the partitions.  The newest should be the current month and the remaining partitions 
   should be for the previous three months.  Part of testing is to run the add future partitions and 
   drop oldest partition month procedures to verify the table is correctly partitioned.
*/

USE `dba_utils`;

-- table to identify partitioned tables and the partitioning frequency / retention
DROP TABLE IF EXISTS  `partition_tbl_list`;

CREATE TABLE `partition_tbl_list`
(
  `id` INT AUTO_INCREMENT NOT NULL
, `table_schema` VARCHAR(64) NOT NULL
, `table_name` VARCHAR(64) NOT NULL
, `p_column_name` VARCHAR(64) NOT NULL
, `p_frequency` VARCHAR(5) NOT NULL
, `p_retention` INT NOT NULL
, `is_active` BIT NOT NULL DEFAULT 1
, `insert_datetime` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
, PRIMARY KEY (`id` ASC)
);

-- tracks procedure execution
DROP TABLE IF EXISTS `partition_activity_log`;
DROP TABLE IF EXISTS `partition_activity_log_aux`;

CREATE TABLE `partition_activity_log` 
(
  `id` INT AUTO_INCREMENT NOT NULL
, `proc_name` varchar(64) NOT NULL
, `start_time` DATETIME NOT NULL
, `end_time` DATETIME NULL
, `start_or_stop` CHAR(5) NOT NULL
, `insert_datetime` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
, PRIMARY KEY (`id` ASC,  `insert_datetime` ASC) 
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY RANGE ( TO_DAYS(insert_datetime))
(PARTITION p_empty  VALUES LESS THAN (0) ENGINE = InnoDB
, PARTITION p_201711 VALUES LESS THAN (TO_DAYS('2017-12-01 00:00:00'))
, PARTITION p_201712 VALUES LESS THAN (TO_DAYS('2018-01-01 00:00:00'))
, PARTITION p_201801 VALUES LESS THAN (TO_DAYS('2018-02-01 00:00:00'))
, PARTITION p_201802 VALUES LESS THAN (TO_DAYS('2018-03-01 00:00:00'))
, PARTITION p_201803 VALUES LESS THAN (TO_DAYS('2018-04-01 00:00:00'))
, PARTITION p_future VALUES LESS THAN MAXVALUE ENGINE = InnoDB
 );

-- tracks steps inside procedure execution
DROP TABLE IF EXISTS `partition_activity_steps_log`;
DROP TABLE IF EXISTS `partition_activity_steps_log_aux`;

CREATE TABLE `partition_activity_steps_log` 
(
  `id` INT AUTO_INCREMENT NOT NULL
, `group_action` varchar(50) NOT NULL
, `table_schema` varchar(64) NOT NULL
, `table_name` varchar(64) NOT NULL
, `step_last_run` varchar(2000) NULL
, `last_activity` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
, `insert_datetime` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
, PRIMARY KEY (`id` ASC, `insert_datetime` ASC)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY RANGE ( TO_DAYS(insert_datetime))
( PARTITION p_empty  VALUES LESS THAN (0) ENGINE = InnoDB
, PARTITION p_201711 VALUES LESS THAN (TO_DAYS('2017-12-01 00:00:00'))
, PARTITION p_201712 VALUES LESS THAN (TO_DAYS('2018-01-01 00:00:00'))
, PARTITION p_201801 VALUES LESS THAN (TO_DAYS('2018-02-01 00:00:00'))
, PARTITION p_201802 VALUES LESS THAN (TO_DAYS('2018-03-01 00:00:00'))
, PARTITION p_201803 VALUES LESS THAN (TO_DAYS('2018-04-01 00:00:00'))
, PARTITION p_future VALUES LESS THAN MAXVALUE ENGINE = InnoDB
 );
 
 -- insert records for the two partitioned management tables
INSERT INTO `partition_tbl_list`
(
  `table_schema`
, `table_name`
, `p_column_name`
, `p_frequency`
, `p_retention`
, `is_active`
)
VALUES 
  ('dba_utils', 'partition_activity_log', 'insert_datetime', 'month', 4, 1) 
, ('dba_utils', 'partition_activity_steps_log', 'insert_datetime', 'month', 4, 1)
;

