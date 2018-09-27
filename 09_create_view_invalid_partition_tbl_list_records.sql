USE `dba_utils`;
  
CREATE VIEW `dba_utils`.`invalid_partition_tbl_list_records` AS 
select `p`.`id` AS `id`,
`p`.`table_schema` AS `table_schema`,
`p`.`table_name` AS `table_name`,
`p`.`p_column_name` AS `p_column_name`,
`p`.`p_frequency` AS `p_frequency`,
`p`.`p_retention` AS `p_retention`,
`p`.`is_active` AS `is_active`,
`p`.`insert_datetime` AS `insert_datetime` 
from (`dba_utils`.`partition_tbl_list` `p` join `information_schema`.`TABLES` `t` 
on (((convert(`p`.`table_schema` using utf8) = `t`.`TABLE_SCHEMA`) 
and (convert(`p`.`table_name` using utf8) = `t`.`TABLE_NAME`)))) 
where ((`t`.`CREATE_OPTIONS` <> 'partitioned') and (`p`.`is_active` = 1));
    
