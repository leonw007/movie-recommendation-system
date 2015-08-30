--Create table and load data
CREATE TABLE users (
  userid INT,
  gender STRING,
  age INT,
  occupation INT,
  zipcode INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'users.dat'
OVERWRITE INTO TABLE users;

CREATE TABLE ratings (
  userid INT,
  movieid INT,
  rate INT,
  timestap INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'ratings.dat'
OVERWRITE INTO TABLE ratings;

CREATE TABLE movies (
  movieid INT,
  title STRING,
  genres STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'movies.dat'
OVERWRITE INTO TABLE movies;



--extract data and save it
INSERT OVERWRITE LOCAL DIRECTORY 'result/data.txt'
SELECT r.movieid, r.rate, u.gender, u.age, u.occupation
FROM users u, ratings r
WHERE u.userid = r.userid
ORDER BY r.movieid, r.rate;

