##### Purpose
The project creates etl pipelines using json logs of Sparkify's user access data, along with json format songs data.
All of this is done in RDBMS environment, since the volume might not be huge, also the since usage will probably be internal for analytics purposes, a bit of downtime wouldn't hurt. 
The results of this project will make accessing all this information easy further down the value chain for eg. for the Analytics team. It can be used to create other valuable tools like a song suggestion mechanism.

##### Database Schema
Star schema is used here, with the songsplay facts table at the centre of the star. And the rest dimensional data are surrounding it.
In terms of Normalised-Forms, the tables are neither extremely normalized or denormalized. The reason for this is to have sooth read (analysis) and write capabilities.

##### How to run
1. Run create_tables.py in the terminal. Ensure there are no errors. This script ensures connection to the database, drops any existing tables, creates new tables.
2. Test the connection using test.ipynb
3. Run etl.py in the terminal, this will get all the required tables loaded to the database.

##### Sample Query Results
With the query below, we will obtain the most popular user_agent.

Query-
select user_agent, count(user_id) from (select distinct user_id, user_agent from songplays) as unique_users group by user_agent order by count(user_id) desc limit 1

Result-
user_agent	count
Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0	9