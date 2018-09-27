#Purpose:  provide an easy way for users to view a table's partitions
USE `dba_utils`;
DROP VIEW IF EXISTS `view_table_partitions`;

CREATE ALGORITHM=MERGE SQL SECURITY DEFINER VIEW `view_table_partitions` AS 

	SELECT `TABLE_SCHEMA`
	, `TABLE_NAME`
	, `PARTITION_NAME`
	, `PARTITION_ORDINAL_POSITION`
	, `PARTITION_METHOD`
	, `PARTITION_EXPRESSION`
	, from_days(`PARTITION_DESCRIPTION`)
    , `TABLE_ROWS`
	FROM `information_schema`.`PARTITIONS`;
