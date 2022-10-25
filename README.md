# adding data to database + ERD + cloud-managed SQL 

# cloud information
MySQL database is set up on Azure.

Need an .env with cloud information to run this repository. The following structure should be used: <br>
MYSQL_HOSTNAME = "inserthere" <br>
MYSQL_USERNAME = "inserthere" <br>
MYSQL_PASSWORD = "inserthere" <br>
MYSQL_DATABASE = "inserthere"

# sql_table_creation.py
1. connect to database 
2. create tables with necessary columns and requirements
3. db.execute to execute queries

# sql_dummy_data.py
1. connect to database
2. create fake data and import real data. clean data and select only columns of interest
3. write insert into query
4. use for loop to insert data into table. db.execute 

# images folder
This folder contains the ERD diagram and 5 sample queries of the database. Each image is labeled as per its content

# data folder
This folder contains the LOINC data which was retrieved from https://loinc.org/downloads/
