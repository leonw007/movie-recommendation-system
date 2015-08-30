
--create a external table
CREATE EXTERNAL TABLE bayesresult (
  movieid INT,
  rate INT,
  male STRING,
  female STRING,
  age1 STRING,
  age18 STRING,
  age25 STRING,
  age35 STRING,
  age45 STRING,
  age50 STRING,
  age56 STRING,
  occ0 STRING,
  occ1 STRING,
  occ2 STRING,
  occ3 STRING,
  occ4 STRING,
  occ5 STRING,
  occ6 STRING,
  occ7 STRING,
  occ8 STRING,
  occ9 STRING,
  occ10 STRING,
  occ11 STRING,
  occ12 STRING,
  occ13 STRING,
  occ14 STRING,
  occ15 STRING,
  occ16 STRING,
  occ17 STRING,
  occ18 STRING,
  occ19 STRING,
  occ20 STRING,
  prob STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/moive';
