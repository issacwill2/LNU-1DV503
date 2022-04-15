import mysql.connector
from csv import reader, writer
import os
import sys
from mysql.connector import errorcode

cnx = mysql.connector.connect(user='root',
                              password='root',
                              host = 'localhost',
                              port = 8889)

DB_NAME = 'aaa'        # database's name
cursor = cnx.cursor()         

# Create a database with the function, but error message if it fails
def make_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed to create a database: {}".format(err))
        exit(1)

# if the DB was not created earlier it does now

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        make_database(cursor)
        print("Database '{}' created now.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

# dictionaries datatype is used below to contain Table and fields/attributes of it.
Table = {}

# Table which holds information of the salary manager
Table['manager'] = (
    "  CREATE TABLE `manager` ("
    "  `managerId` int(11) NOT NULL,"
    "  `FirstName` varchar(45) NOT NULL,"
    "  `LastName` varchar(45) NOT NULL,"
    "  `Gender` char(1) NOT NULL,"
    "  `Age` int(11) NOT NULL,"
    "  `Phone` varchar(45) NOT NULL,"
    "  `Email` varchar(45) NOT NULL,"
    "  `Adddress` varchar(45) NOT NULL,"
    "  `salaryAmount` varchar(45) NOT NULL,"
    "  PRIMARY KEY (`managerId`)"
    ")  ENGINE = InnoDB")

# Table which holds information of the employee
Table['employee'] = (
    "  CREATE TABLE `employee` ("
    "  `employeeId` int(11) NOT NULL,"
    "  `FirstName` varchar(45) NOT NULL,"
    "  `LastName` varchar(45) NOT NULL,"
    "  `Gender` char(1) NOT NULL,"
    "  `Age` int(11) NOT NULL,"
    "  `Phone` varchar(11) NOT NULL,"
    "  `Address` varchar(20) NOT NULL,"
    "  `Email` varchar(45) NOT NULL,"
    "  `salaryAmount` varchar(45) NOT NULL,"
    "  PRIMARY KEY (`employeeId`)"
    ") ENGINE=InnoDB")

# Table holds the paid out salary
Table['salary'] = (
    "  CREATE TABLE `salary` ("
    "  `salaryCode` int(11) NOT NULL,"
    "  `managerId` int(11) NOT NULL," 
    "  `salaryAmount` varchar(45) NOT NULL,"
    "  PRIMARY KEY (`salaryCode`),"
    "  CONSTRAINT `managerId` FOREIGN KEY (`managerId`)"
    "  REFERENCES `manager` (`managerId`) ON DELETE NO ACTION ON UPDATE NO ACTION"
   ") ENGINE = InnoDB")


# salary bank table holds different salary banks the salary bank
Table['salaryBank'] = (
    "  CREATE TABLE `salaryBank` ("
    "  `salaryBankId` int(11) NOT NULL,"
    "  `Name` varchar(45) NOT NULL,"
    "  `Phone` varchar(45) NOT NULL,"
    "  `Address` varchar(45) NOT NULL,"
    "  `employeeId` int(11),"
    "  `salaryCode` int(11),"
    "  PRIMARY KEY (`salaryBankId`),"
    "  CONSTRAINT `salaryCode` FOREIGN KEY (`salaryCode`)"
    "  REFERENCES `salary` (`salaryCode`) ON DELETE NO ACTION ON UPDATE NO ACTION,"
    "  CONSTRAINT `employeeId` FOREIGN KEY (`employeeId`)"
    "  REFERENCES `employee` (`employeeId`) ON DELETE NO ACTION ON UPDATE NO ACTION"
    ") ENGINE=InnoDB")

# The for loop Iterates the Table to make new one or responding that it existed.
for name_table in Table:
    define_table = Table[name_table]
    try:
        print("Creating table {}: ".format(name_table), end='')
        cursor.execute(define_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("exists already.")
        else:
            print(err.msg)
    else:
        print("it is created now")

# Open files
managers = open("manager.csv")
employees = open("employee.csv")
salary = open("salary.csv")
salaryBanks = open("salaryBank.csv")

# Read file 
csvReader = reader(managers)
next(csvReader)   #next row

# inserting the values by specifying the number of the columns below
for row in csvReader:
    try:
        cursor.execute("INSERT INTO manager VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])   
        cnx.commit()
    except:
        continue

# Reading file
csvReader = reader(employees)
next(csvReader)
for row in csvReader:
    try:
        cursor.execute("INSERT INTO employee VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])   
        cnx.commit()
    except:
        continue

csvReader = reader(salary)
next(csvReader) 
for row in csvReader:
    try:
        cursor.execute("INSERT INTO salary VALUES (%s,%s,%s)",
            [row[0],row[1],row[2]])   
        cnx.commit()
    except:
        continue

csvReader = reader(salaryBanks)
next(csvReader) 
for row in csvReader:
    try:
        cursor.execute("INSERT INTO salaryBank VALUES (%s,%s,%s,%s,%s,%s)",
            [row[0],row[1],row[2],row[3],row[4],row[5]])   
        cnx.commit()
    except:
        continue


print("===============================================================")  
print("Selecting first name and last name of manager and employee having the same salaryAmount\n")
# Query select only the first name and last name of manager and employee having the same salaryAmount
query = """SELECT manager.managerId, manager.FirstName,\
    manager.LastName, employee.employeeId, employee.FirstName, \
    employee.LastName
   FROM manager, employee
   WHERE manager.salaryAmount = employee.salaryAmount"""
cursor.execute(query)
my_result = cursor.fetchall()

for row in my_result:
    print(row)         

print("===============================================================")  
print("Selecting everything from salarybank table\n")
query = "SELECT * FROM salarybank"
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)


print("=====================================================================")  
print("CROSS JOIN\n")
# CROSS JOIN
query = """SELECT manager.managerId, manager.FirstName,\
    manager.LastName, employee.employeeId,\
    employee.FirstName, employee.LastName
    FROM manager CROSS JOIN employee \
    WHERE manager.salaryAmount = employee.salaryAmount"""
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)


print("=============================================================================")  
print("VIEW\n")
# Creating view and concatinatin two tables
query = """CREATE VIEW new AS SELECT CONCAT('d', managerId)\
    AS id, manager.FirstName, manager.LastName, \
    'manager' AS status
    FROM manager
    UNION SELECT CONCAT('p', employeeId) AS id, \
    employee.FirstName, employee.LastName, 'employee' AS status
    FROM employee"""
try:
    cursor.execute(query)
    cnx.commit()
except:
    cnx.rollback()    
cursor.execute("SELECT * FROM new ORDER BY FirstName")

for x in cursor:
    print(x)

print("=============================================================================")  
print("INNER JOIN\n")
# INNER JOIN on salary and employee table
query = """SELECT * 
           FROM salary
           INNER JOIN employee
           ON salary.salaryCode = employee.employeeId
           """
cursor.execute(query)
my_result = cursor.fetchall()
for row in my_result:
    print(row)
print("=============================================================================")


cursor.close()
cnx.commit()
cnx.close()
