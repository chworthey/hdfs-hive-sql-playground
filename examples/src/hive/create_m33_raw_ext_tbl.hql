CREATE EXTERNAL TABLE m33_raw (row_str STRING)
  COMMENT 'This is a comment'
  PARTITIONED BY (peculiarity STRING)
  ROW FORMAT DELIMITED
  STORED AS TEXTFILE
  TBLPROPERTIES ("skip.header.line.count"="3");

ALTER TABLE m33_raw
  ADD PARTITION (peculiarity = 'cp')
  LOCATION '/data/m33_0.01/cp';

ALTER TABLE m33_raw
  ADD PARTITION (peculiarity = 'nocp')
  LOCATION '/data/m33_0.01/nocp';