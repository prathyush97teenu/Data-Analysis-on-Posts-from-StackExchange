-- Register the jar file piggybank.jar
register '/usr/lib/pig/piggybank.jar';

-- For loading CSV files with support of multi-line fields
define CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Loading the data into Pig
csv1 = load 'gs://dataproc-staging-us-central1-795277444073-rzswho19/cleaned_data/QueryResult1.csv' using CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

csv2 = load 'gs://dataproc-staging-us-central1-795277444073-rzswho19/cleaned_data/QueryResult2.csv' using CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

csv3 = load 'gs://dataproc-staging-us-central1-795277444073-rzswho19/cleaned_data/QueryResult3.csv' using CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

csv4 = load 'gs://dataproc-staging-us-central1-795277444073-rzswho19/cleaned_data/QueryResult4.csv' using CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray, ContentLicense:chararray);

-- Joining all the four processed CSV files using the UNION command
union_data = UNION csv1,csv2,csv3,csv4;

-- Keeping only the necessary columns for processing
required_data = FOREACH union_data GENERATE Id AS Id, Score AS Score, ViewCount AS ViewCount, Body AS Body, OwnerUserId AS OwnerUserId, Title AS Title, Tags AS Tags;

-- Filtering data to remove all the null values from Score and OwnerUserId columns
filter_data = FILTER required_data BY (OwnerUserId is NOT NULL);
final_data = FILTER filter_data BY (Score IS NOT NULL);

-- Using the STORE command to store the processed data in the Google Cloud Staging Bucket
STORE final_data INTO 'gs://dataproc-staging-us-central1-795277444073-rzswho19/processed_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');
