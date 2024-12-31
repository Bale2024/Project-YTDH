Guvi's first Capstone Project: You Tube Data Harvesting and Warehousing.
* About:The project aims at extracting data from channels available on "YOUTUBE", a social media platform and a community where 
lots of videos and comments will be made based on the different type of contents.
1.Extraction is performed using a Google API key created on Google Developer Console,using a valid Gmail Account.
After extracting data(response) through requests, the necessary data(partial resource) is validated.
A connection is established using connection parameters like username, password, database etc, and a cursor object is 
instantiated which plays a key role in transfering data and executing queries.
2.The data is then transferred to SQL database, Postgres SQL(personalised workbench),where tables are created with different
attributes and then the data is inserted into respective tables using different insert methods, done by python script 
in VS code platform.
Queries are performed on tables by performing some basic SQL commands and join operation as well to generate insights about
the Channel's Data stored in the Database.
3.The Streamlit App, a front end Application is finally developed where a dashboard is generated and a user is asked to enter
a particular channel_id to perform Data Migration to SQL Database.Later, user can choose a question among the set of listed
questions provided to generate insights about the data of YouTube Channels.
4.Finally, I conclude that through this Capstone project(based on Social media Domain), learnings include scraping data 
through methods, establishing a connection and about its parameters, creating tables with different column headers,
inserting data into tables, retrieving the rows using SQL queries and development of a frontend dashboard to view the 
insights and display the data in form of tables to the user for the concerned queries as a part of Data Visualisation.
