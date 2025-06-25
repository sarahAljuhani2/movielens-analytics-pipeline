-- Snowflake user creation
-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 4: Create the `dbt` user and assign to the transform role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;

-- Step 5: Create a database and schema for the MovieLens project
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;

--------------------------------------------New sheet-----------------------------------------------

-- Set defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVIELENS;
USE SCHEMA RAW;



CREATE STAGE netflixstage
  URL='s3://netflixdataset-sarah'
  CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');


  -- Load raw_movies
CREATE OR REPLACE TABLE raw_movies (
  movieId INTEGER,
  title STRING,
  genres STRING
);


COPY INTO raw_movies
FROM '@netflixstage/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');



-- Load raw_ratings
CREATE OR REPLACE TABLE raw_ratings (
  userId INTEGER,
  movieId INTEGER,
  rating FLOAT,
  timestamp BIGINT
);

COPY INTO raw_ratings
FROM '@netflixstage/ratings.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_tags
CREATE OR REPLACE TABLE raw_tags (
  userId INTEGER,
  movieId INTEGER,
  tag STRING,
  timestamp BIGINT
);

COPY INTO raw_tags
FROM '@netflixstage/tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Load raw_genome_scores
CREATE OR REPLACE TABLE raw_genome_scores (
  movieId INTEGER,
  tagId INTEGER,
  relevance FLOAT
);

COPY INTO raw_genome_scores
FROM '@netflixstage/genome-scores.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_genome_tags
CREATE OR REPLACE TABLE raw_genome_tags (
  tagId INTEGER,
  tag STRING
);

COPY INTO raw_genome_tags
FROM '@netflixstage/genome-tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_links
CREATE OR REPLACE TABLE raw_links (
  movieId INTEGER,
  imdbId INTEGER,
  tmdbId INTEGER
);

COPY INTO raw_links
FROM '@netflixstage/links.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');



-------------------------------New sheet-----------------------------------------------

drop view DIM_MOVIES;
drop view src_genome_score;
drop view src_genome_tags;
drop view src_links;
drop view src_movies;
drop view src_ratings;
drop view src_tags;


-------------------------------New sheet-----------------------------------------------


SELECT * FROM MOVIELENS.DEV.FCT_RATINGS
ORDER BY rating_timestamp DESC 
LIMIT 5;

SELECT * FROM MOVIELENS.DEV.SRC_RATINGS
ORDER BY rating_timestamp DESC 
LIMIT 5;

INSERT INTO MOVIELENS.DEV.SRC_RATINGS (user_id, movie_id, rating, rating_timestamp) 

VALUES (87587, '7151', '4.0','2015-03-31 23:40:02.000 -0700 ')



-------------------------------New sheet-----------------------------------------------



select * from snapshots.snap_tags
WHERE user_id=18
order by user_id, dbt_valid_from DESC;

UPDATE src_tags
SET tag = 'Mark Waters Returns',tag_timestamp=CAST(CURRENT_TIMESTAMP() as TIMESTAMP_NTZ)
WHERE user_id=18;

select * from dev.src_tags
WHERE user_id=18;


-------------------------------New sheet-----------------------------------------------

CREATE OR REPLACE TABLE MOVIELENS.DEV.MOVIE_ANALYSIS AS (

  WITH ratings_summary AS (
    SELECT
      movie_id,
      AVG(rating) AS average_rating,
      COUNT(*) AS total_ratings,
      MIN(rating) AS min_rating,
      MAX(rating) AS max_rating,
      STDDEV(rating) AS rating_stddev,
      MIN(rating_timestamp) AS first_rating,
      MAX(rating_timestamp) AS last_rating
    FROM MOVIELENS.DEV.FCT_RATINGS
    WHERE rating BETWEEN 0.5 AND 5.0
    GROUP BY movie_id
    HAVING COUNT(*) > 100
  ),

  genre_breakdown AS (
    SELECT
      movie_id,
      genres,
      ARRAY_SIZE(SPLIT(genres, '|')) AS genre_count
    FROM MOVIELENS.DEV.DIM_MOVIES
  ),

  tag_counts AS (
    SELECT
      movie_id,
      COUNT(*) AS tag_count
    FROM MOVIELENS.DEV.SRC_TAGS
    GROUP BY movie_id
  ),

  final AS (
    SELECT
      m.movie_id,
      m.movie_title,
      m.genres,
      gb.genre_count,
      rs.average_rating,
      rs.total_ratings,
      rs.min_rating,
      rs.max_rating,
      rs.rating_stddev,
      rs.first_rating,
      rs.last_rating,
      CAST(rs.first_rating AS DATE) AS first_rating_date,
      CAST(rs.last_rating AS DATE) AS last_rating_date,
      COALESCE(tc.tag_count, 0) AS tag_count,
      CASE 
        WHEN rs.average_rating >= 4.5 THEN 'Excellent'
        WHEN rs.average_rating >= 3.5 THEN 'Good'
        WHEN rs.average_rating >= 2.5 THEN 'Average'
        ELSE 'Poor'
      END AS rating_category
    FROM ratings_summary rs
    JOIN MOVIELENS.DEV.DIM_MOVIES m ON m.movie_id = rs.movie_id
    LEFT JOIN genre_breakdown gb ON gb.movie_id = m.movie_id
    LEFT JOIN tag_counts tc ON tc.movie_id = m.movie_id
  )

  SELECT * FROM final
  ORDER BY average_rating DESC

);
