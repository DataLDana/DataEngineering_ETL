# Building up a ETL pipeline for a fictional company
The project for Gans, a hypothetical e-scooter-sharing company, involved building a data pipeline to predict scooter movement using real-time data like weather, flight schedules, and population trends. Within this use case of my Datascience Bootcamp at WBS Coding school, I created a relational database in MySQL, populated it with dynamic and static data from external APIs, and automated the process using Google Cloud Functions and Cloud Scheduler. Key tasks included schema design, managing relationships between static (cities, airports) and dynamic tables (weather, flights, populations), looping through JSON responses and storing data in pandas dataframes.
For further insights about the project see this medium article.
https://medium.com/@data-dana-l/data-engineering-through-the-eyes-of-a-child-d25b3a29ff9b

## Languages and Libraries Used
* Python: pandas, datetime, requests, sqlalchemy, pymysql
* MySQL
  
## Key Learnings
* Data collection with APIs
* Database Design: Structuring relational databases with both static and dynamic data. Handling many-to-many relationships (like cities and airports) 
* Transforming data into usable formats with loops
* Building nested code with functions and querying the database within a function
* Cloud Infrastructure: Learning how to migrate a local setup to the cloud
* Cloud Automation with Google Scheduler
* Error Handling and Debugging with lot's of patience

## Challenges Overcame
* Navigating trough the GCP labyinth settings
* Not to loose track when to send/receive data from the database  

## Notes on how to run the Script
Files:

**keys.py**
- insert your API keys
- add information of SQL Instance (Google Cloud Platform)

**Gans_SQL_GCP.md**
- SQL Code to set up the relational database structure

**GansETL_python_GCP.ipynb**
- main function, run to create static tables (cities, airports)
- also shows the code which is run on GPC
- pull current database information from the cloud

**functions_GCP**
- contains seperate functions to create the cities, airport, population, weather and flights table
- function 'only_add_new', which ensures that only new data from a DataFrame is sent to SQL

