import mysql.connector
from csv import reader, writer
import os
import sys
from mysql.connector import errorcode

# Connecting mysql to the host
cnx = mysql.connector.connect(user='root',
                              password='123456',
                              host='127.0.0.1')


DB_NAME = 'william'               # Naming the database
cursor = cnx.cursor()             # Creating cursor of connection for executing queries and commands


# Creating the database using function and raise error message if creating fails
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


# Error handling and message if database does not exist or created successfully

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database '{}' created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)


# Creating dictionaries variable for holding the tables and it will have the attributes of each table
TABLES = {}

# Tabble containing the information about the blood donors
TABLES['donors'] = (
    "  CREATE TABLE `donors` ("
    "  `DonorId` int(11) NOT NULL,"
    "  `FirstName` varchar(45) NOT NULL,"
    "  `LastName` varchar(45) NOT NULL,"
    "  `Gender` char(1) NOT NULL,"
    "  `Age` int(11) NOT NULL,"
    "  `Phone` varchar(45) NOT NULL,"
    "  `Email` varchar(45) NOT NULL,"
    "  `BloodGroup` varchar(45) NOT NULL,"
    "  `Adddress` varchar(45) NOT NULL,"
    "  `Message` nvarchar(4000) NULL,"
    "  PRIMARY KEY (`DonorId`)"
    ")  ENGINE = InnoDB")

# Table containing  information about the patients
TABLES['patients'] = (
    "  CREATE TABLE `patients` ("
    "  `PatientId` int(11) NOT NULL,"
    "  `FirstName` varchar(45) NOT NULL,"
    "  `LastName` varchar(45) NOT NULL,"
    "  `Gender` char(1) NOT NULL,"
    "  `Age` int(11) NOT NULL,"
    "  `Phone` varchar(11) NOT NULL,"
    "  `BloodGroup` varchar(45) NOT NULL,"
    "  `Address` varchar(20) NOT NULL,"
    "  `Email` varchar(45) NOT NULL,"
    "  PRIMARY KEY (`PatientId`)"
    ") ENGINE=InnoDB")

# Table contains the donated blood
TABLES['blood'] = (
    "  CREATE TABLE `blood` ("
    "  `BloodCode` int(11) NOT NULL,"
    "  `BloodGroup` varchar(45) NOT NULL,"
    "  `DonorId` int(11) NOT NULL,"
    "  PRIMARY KEY (`BloodCode`),"
    "  INDEX `DonorId_idx` (`DonorId` ASC) VISIBLE,"
    "  CONSTRAINT `DonorId` FOREIGN KEY (`DonorId`)"
    "  REFERENCES `donors` (`DonorId`) ON DELETE NO ACTION  ON UPDATE NO ACTION"
   ")ENGINE = InnoDB")


# Blood bank table containging different blood banks the blood bank
TABLES['bloodbank'] = (
    "  CREATE TABLE `bloodbank` ("
    "  `BloodBankId` int(11) NOT NULL,"
    "  `Name` varchar(45) NOT NULL,"
    "  `Phone` varchar(45) NOT NULL,"
    "  `Address` varchar(45) NOT NULL,"
    "  `BloodCode` int(11),"
    "  `PatientId` int(11),"
    "  PRIMARY KEY (`BloodBankId`),"
    "  INDEX `PatientId_idx` (`PatientId` ASC) VISIBLE,"
    "  CONSTRAINT `BloodCode` FOREIGN KEY (`BloodCode`)"
    "  REFERENCES `blood` (`BloodCode`) ON DELETE NO ACTION ON UPDATE NO ACTION,"
    "  CONSTRAINT `PatientId` FOREIGN KEY (`PatientId`)"
    "  REFERENCES `patients` (`PatientId`) ON DELETE NO ACTION ON UPDATE NO ACTION"
    ") ENGINE=InnoDB")


# Iterating through Tables to create or giving message if already exists
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Opening the CSV files for inserting the data
donatorVariable = open("donors.csv")
patientsVariable = open("patients.csv")
blood = open("blood.csv")
bloodBank_Var = open("blood_bank.csv")

# Read the file 
csvReader = reader(donatorVariable)
next(csvReader)   # Jumps to he next row where the actual data starts

# Loops through csvReader and inserts the data row by row in to planets table 
# Based on the number of columns they have
for row in csvReader:
    try:
        cursor.execute("INSERT INTO donors VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]])   
        cnx.commit()
    except:
        continue


# Reading from csv file to patients table
csvReader = reader(patientsVariable)
next(csvReader)
# Loops through csvReader and inserts the data row by row in to patients table 
for row in csvReader:
    try:
        cursor.execute("INSERT INTO patients VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])   
        cnx.commit()
    except:
        continue

# inserting values from csv file to donated blood table
csvReader = reader(blood)
next(csvReader) 
for row in csvReader:
    try:
        cursor.execute("INSERT INTO blood VALUES (%s,%s,%s)",
            [row[0],row[1],row[2]])   
        cnx.commit()
    except:
        continue

# inserting values from csv file to Blood bank table
csvReader = reader(bloodBank_Var)
next(csvReader) 
for row in csvReader:
    try:
        cursor.execute("INSERT INTO bloodbank VALUES (%s,%s,%s,%s,%s,%s)",
            [row[0],row[1],row[2],row[3],row[4],row[5]])   
        cnx.commit()
    except:
        continue
'''

# Selects first name, last name phone from donors table
query = "SELECT FirstName, LastName, Phone FROM blood_donation.donors"
cursor.execute(query)
my_result = cursor.fetchall()

for row in my_result:
    print(row)


print("===============================================================")  
print("Fetching all information of donors and patients which have in the same blood group\n")


# Query selecting everything fro donors and patiens tables having the same blood group
query = "SELECT * FROM donors, patients WHERE donors.BloodGroup = 'O+' AND patients.BloodGroup = 'O+'"
cursor.execute(query)
my_result = cursor.fetchall()

for row in my_result:
    print(row)


print("===============================================================")  
print("Selecting first name and last name of donors and patiets having the same blood group\n")
# Query selecting only the first name and last name of donors and patients having the same blood group
query = """SELECT donors.DonorId, donors.FirstName, donors.LastName, patients.PatientId, patients.FirstName, patients.LastName
   FROM donors, patients
   WHERE donors.BloodGroup = patients.BloodGroup"""
cursor.execute(query)
my_result = cursor.fetchall()

for row in my_result:
    print(row)         


print("===============================================================")  
print("Selecting everything from bloodbank table\n")
# Query on the blood bank table which matches and fetches patients and donor having
# The same blood by matching through patients Id from patients table and Blood code
# from the blood donation table

query = "SELECT * FROM bloodbank"
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)


print("===============================================================")  
print("LEFT JOIN\n")
# LEFT JOIN
# This join returns all the rows of the table on the left side of the join and matching rows for the table on the right side of join.
# In this case donors and patients whose blood group match
query = """SELECT donors.FirstName AS Donor_fn, donors.LastName AS Donor_ln, donors.BloodGroup AS Donor_bg, patients.FirstName AS Patient_fn,
           patients.LastName AS Patient_ln, patients.BloodGroup AS Patient_bg FROM blood_donation.donors LEFT JOIN blood_donation.patients 
           ON donors.BloodGroup = patients.BloodGroup WHERE donors.BloodGroup = 'O+' AND patients.BloodGroup = 'O+'"""
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)


print("=====================================================================")  
print("CROSS JOIN\n")
# CROSS JOIN
# A cross join returns the Cartesian product of rows from the rowsets in the join. 
# In other words, it will combine each row from the first rowset with each row from the second rowset.
# For each record in the first table, all the records in the second table are joined
query = """SELECT donors.DonorId, donors.FirstName, donors.LastName, patients.PatientId, patients.FirstName, patients.LastName
          FROM donors CROSS JOIN patients WHERE donors.BloodGroup = patients.BloodGroup"""
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)


print("=============================================================================")  
print("AGGREGATE FUNCTION COUNT\n")
# Counting the blood group based on Gender
query = """SELECT COUNT(*), donors.BloodGroup, donors.Gender
           FROM blood_donation.donors
           GROUP BY donors.BloodGroup, donors.Gender
           ORDER BY donors.FirstName desc"""
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)


print("=============================================================================")  
print("VIEW\n")
# Creating view and concatinatin two tables
query = """CREATE VIEW new AS SELECT CONCAT('d', DonorId) AS id, donors.FirstName, donors.LastName, 'Donor' AS status
           FROM donors
           UNION SELECT CONCAT('p', PatientId) AS id, patients.FirstName, patients.LastName, 'Patient' AS status
           FROM patients"""
try:
    cursor.execute(query)
    cnx.commit()
except:
    cnx.rollback()    
cursor.execute("SELECT * FROM new ORDER BY FirstName")

for x in cursor:
    print(x)
'''

print("=============================================================================")  
print("INNER JOIN\n")
# INNER JOIN on blood table and patients
query = """SELECT * 
           FROM blood
           INNER JOIN patients
           ON blood.BloodCode = patients.PatientId
           """
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)

cursor.close()
cnx.commit()
cnx.close()
