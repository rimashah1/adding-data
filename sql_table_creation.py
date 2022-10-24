import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
MYSQL_HOSTNAME = os.getenv("MYSQL_HOSTNAME")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")


connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
db = create_engine(connection_string)

tableNames = db.table_names()

table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_treatments_procedures = """
create table IF NOT EXISTS treatments_procedures (
  id int auto_increment,
  cpt_codes varchar(255) default null unique,
  treatments_procedures_desciption varchar(255),
  PRIMARY KEY (id)
  );
  """

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_social_determinants = """
create table IF NOT EXISTS social_determinants (
  id int auto_increment,
  loinc_codes varchar(255) default null unique,
  social_determinants_description varchar(255)
  PRIMARY KEY (id)
  );
  """

db.execute(table_patients)
db.execute(table_medications)
db.execute(table_treatments_procedures)
db.execute(table_conditions)
db.execute(table_social_determinants)


tableNames = db.table_names()