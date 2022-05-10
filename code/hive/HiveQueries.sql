-- Before querying, load processed_data into the local file system and then on to HDFS.
-- hdfs dfs -get "gs://dataproc-staging-us-central1-795277444073-rzswho19/processed_data" "/home/teenu_prathyush2/"
-- hdfs dfs -put "/home/teenu_prathyush2/processed_data/" "/user/pig"

-- To open hive terminal
hive;

-- To print the headers
set hive.cli.print.header=true;

-- Creating a table called as stack_posts using the create table command
CREATE TABLE IF NOT EXISTS stack_posts(Id INT, Score INT, ViewCount INT, Body STRING, OwnerUserId STRING, Title STRING, Tags STRING)  ROW FORMAT DELIMITED  FIELDS TERMINATED BY ','  LOCATION '/user/pig/processed_data';

-- Querying the table to check if the data has been loaded properly
SELECT Id, Score FROM stack_posts LIMIT 10;

-- Task 2.2.1. The top 10 posts by score
SELECT Id, Score, ViewCount, OwnerUserId, Title FROM stack_posts ORDER BY Score DESC LIMIT 10;

-- Task 2.2.2. The top 10 users by post score 
SELECT OwnerUserId, SUM(Score) AS TOTAL_SCORE FROM stack_posts GROUP BY OwnerUserId ORDER BY TOTAL_SCORE DESC LIMIT 10;

-- Task 2.2.3. The number of distinct users, who used the word “cloud” in one of their posts 
SELECT COUNT(DISTINCT OwnerUserId) AS distinct_users_count FROM stack_posts WHERE(lower(body) LIKE '%cloud%' OR lower(title) LIKE '%cloud%' OR lower(tags) LIKE '%cloud%');

-- Creating a table called as top_users_scores to store the top 10 users by post scores
CREATE TABLE IF NOT EXISTS top_users_scores(OwnerUserId INT, TotalScore INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
INSERT INTO top_users_scores SELECT OwnerUserId, SUM(Score) AS TOTAL_SCORE FROM stack_posts GROUP BY OwnerUserId ORDER BY TOTAL_SCORE DESC LIMIT 10;

-- Creating another table called as top_users_posts to store the text content from all of the top 10 users
CREATE TABLE IF NOT EXISTS top_users_posts(OwnerUserId INT, Body STRING, Title STRING, Tags STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
INSERT INTO top_users_posts SELECT OwnerUserId, Body, Title, Tags FROM stack_posts WHERE OwnerUserId IN (SELECT OwnerUserId from top_users_scores) GROUP BY OwnerUserID, Body, Title, Tags;

-- Copying the data into HDFS
INSERT OVERWRITE DIRECTORY '/user/hive/stack_data' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT OwnerUserId, Body, Title, Tags FROM top_users_posts GROUP BY OwnerUserId, Body, Title, Tags;
