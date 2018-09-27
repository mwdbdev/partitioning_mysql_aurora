# partitioning_mysql_aurora
partitioning code for MySQL and Aurora MySQL clusters

The code in this repository manages tables partitioned by month, based on the value of a date column named "insert_datetime".  The monthly partitioning scheme is suitable for data that is organized around transaction date.

The code consists of 10 files that create the host schema, the partition management tables, the logging tables, the procedures to add and drop partition months, procedures to script out the partition clause for an existing table, verify the necessary partition columns exist (or add them), and views to see partitions and find records in the partition_tbl_list table for any tables that are not yet partitioned.
