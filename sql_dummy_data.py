import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random

load_dotenv()
MYSQL_HOSTNAME = os.getenv("MYSQL_HOSTNAME")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")


connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
db = create_engine(connection_string)




######### create fake PATIENT INFO ###########
fake = Faker()
fake_patients = [
    {
        'mrn': str(uuid.uuid4())[:8],  #keep the first 8 characters of the uuid
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(50)]

# put fake data into df to make it more readable
df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn in case 
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])



######### create real ICD10 CODES ###########
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']] # make smaller df with columns of interest
icd10codesShort_1k = icd10codesShort.sample(n=1000) # random sample 1k rows
# drop duplicates from icd10codesShort_1k
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')



######### create real NDC CODES ###########
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1) # random sample 1k rows
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')



######### create real CPT CODES ###########
cpt_codes = pd.read_csv("https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv")
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1) # random sample 1k rows
# drop duplicates from cpt_codes_1k
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')



######### create real LOINC CODES ###########
loinc_codes = pd.read_csv("data\Loinc.csv")
loinc_codes_short = loinc_codes[['LOINC_NUM', 'LONG_COMMON_NAME']] # make smaller df with columns of interest
loinc_codes_1k = loinc_codes_short.sample(n=1000, random_state=1) # random sample 1k rows
# drop duplicates from cpt_codes_1k
loinc_codes_1k = loinc_codes_1k.drop_duplicates(subset=['LOINC_NUM'], keep='first')




######### INSERT FAKE PATIENTS ###########

# query has the column names and then values as wildcards
insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    # execute query and add in each fake patient info to the wildcard in the sql query above
    db.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index) # to show progress

# query dbs to see if data is there
df = pd.read_sql_query("SELECT * FROM patients", db)



######### INSERT CONDITIONS/ICD10##########

insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df = pd.read_sql_query("SELECT * FROM conditions", db)



######### INSERT MEDICATIONS/NDC ###########

insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"

startingRow = 0
for index, row in ndc_codes_1k.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row db: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df = pd.read_sql_query("SELECT * FROM medications", db)



######### INSERT TREATMENTS-PROCEDURES/CPT ###########

insertQuery = "INSERT INTO treatments_procedures (cpt_codes, treatments_procedures_desciption) VALUES (%s, %s)"

startingRow = 0
for index, row in cpt_codes_1k.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row db: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df = pd.read_sql_query("SELECT * FROM treatments_procedures", db)



######### INSERT SOD/LOINC ###########

insertQuery = "INSERT INTO social_determinants (social_determinants_description, loinc_codes) VALUES (%s, %s)"

startingRow = 0
for index, row in loinc_codes_1k.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['LONG_COMMON_NAME'], row['LOINC_NUM']))
    print("inserted row db: ", index)
    ## stop once we have 100 rows
    if startingRow == 100:
        break

# query dbs to see if data is there
df = pd.read_sql_query("SELECT * FROM social_determinants", db)



######### INSERT FAKE PATIENT CONDITIONS ###########

# query conditions and patients to get the ids
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)

# create a empty dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

print(df_patient_conditions.head(20))

# add a random condition to each patient
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)




######### INSERT FAKE PATIENT MEDICATIONS ###########

df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])
# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(10))

insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)