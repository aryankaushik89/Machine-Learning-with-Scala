CREATE DATABASE movielens;

USE movielens;

CREATE EXTERNAL TABLE movielens.udata_landing (
   user_id INT,
   item_id INT,
   rating INT,
   ts BIGINT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
LOCATION '/user/root/lab/Assignment1/udata_landing/';

CREATE EXTERNAL TABLE movielens.udata (
   user_id INT,
   item_id INT,
   rating INT,
   ts TIMESTAMP
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
LOCATION '/user/root/lab/Assignment1/udata/';

INSERT OVERWRITE TABLE movielens.udata 
Select user_id, item_id, rating, CAST(from_unixtime(ts) AS TIMESTAMP) as ts 
FROM movielens.udata_landing;


CREATE EXTERNAL TABLE movielens.uitem_landing (
  movie_id INT,
  movie_title STRING,
  release_date STRING,
  video_release_date STRING,
  imdb_url STRING,
  unknown INT,
  action INT,
  adventure INT,
  animation INT,
  childrens INT,
  comedy INT,
  crime INT,
  documentary INT,
  drama INT,
  fantasy INT,
  film_noir INT,
  horror INT,
  musical INT,
  mystery INT,
  romance INT,
  sci_fi INT,
  thriller INT,
  war INT,
  western INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n'
LOCATION '/user/root/lab/Assignment1/uitem_landing/';


CREATE EXTERNAL TABLE movielens.uitem (
  movie_id INT,
  movie_title STRING,
  release_date TIMESTAMP,
  video_release_date STRING,
  imdb_url STRING,
  unknown INT,
  action INT,
  adventure INT,
  animation INT,
  childrens INT,
  comedy INT,
  crime INT,
  documentary INT,
  drama INT,
  fantasy INT,
  film_noir INT,
  horror INT,
  musical INT,
  mystery INT,
  romance INT,
  sci_fi INT,
  thriller INT,
  war INT,
  western INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n'
LOCATION '/user/root/lab/Assignment1/uitem/';


INSERT OVERWRITE TABLE movielens.uitem 
Select 
  movie_id, 
  movie_title, 
  CAST(from_unixtime(unix_timestamp(release_date, 'dd-MMM-yyyy')) AS TIMESTAMP) as release_date,
  video_release_date,
  imdb_url,
  unknown,
  action,
  adventure,
  animation,
  childrens,
  comedy,
  crime,
  documentary,
  drama,
  fantasy,
  film_noir,
  horror,
  musical,
  mystery,
  romance,
  sci_fi,
  thriller,
  war,
  western
FROM movielens.uitem_landing;

