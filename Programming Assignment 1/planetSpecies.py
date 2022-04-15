import mysql.connector
import csv
import os

cnx = mysql.connector.connect(user='root', password='root',host='127.0.0.1')

cursor = cnx.cursor()
DbName = 'Tesfagiorgish'
filepath = os.getcwd()
print(filepath)

try:
  cursor.execute('use ' + DbName)
# Exception if the database not found and to create it.
except mysql.connector.Error as e:
  Db = input("database :" + DbName +
    " is not found, Do you want to create it now?(Y/N): ")

  if Db == "Y" or Db == "y":
      
    # Creates One new database and Two tables for planets and species.
    table_planets = "CREATE TABLE planets (name VARCHAR(255) NOT NULL,\
      rotation_period INT,orbital_period INT,diameter INT, climate\
      nvarchar(20),gravity nvarchar(20),terrain nvarchar(255),surface_water \
      INT,population INT,primary key(name));"
      
    table_species = "CREATE TABLE species (sp_name VARCHAR(255) NOT NULL,\
      classification nvarchar(50),designation nvarchar(50),average_height \
      varchar(50),skin_colors nvarchar(50),hair_colors nvarchar(50),eye_colors \
      nvarchar(50),average_lifespan INT,language nvarchar(50),homeworld \
      nvarchar(50),primary key(sp_name));"
    
    cursor.execute("create database " + DbName + ";")
    cursor.execute("use " + DbName + ";")
    
    cursor.execute(table_planets)
    cursor.execute(table_species)
    
    cursor.execute('SET GLOBAL sql_mode ="";')
    cursor.execute('SET SESSION sql_mode ="";')

    openFile_planet = open('planets.csv') # To open planet csv file.
    filePlanets = csv.reader(openFile_planet)
    skip_over = True

    for row in filePlanets:
      if skip_over:
        skip_over = False
        continue
      cursor.execute('INSERT INTO planets(name, rotation_period,orbital_period,\
        diameter, climate, gravity, terrain, surface_water, \
        population)' 'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
      cursor.execute("commit;")

    openFile = open('species.csv') # To open species csv file.
    skip_over = True
    fileSpecies = csv.reader(openFile)

    for row in fileSpecies:
      if skip_over:
        skip_over = False
        continue
      cursor.execute('INSERT INTO species(sp_name, classification, designation,\
        average_height, skin_colors, hair_colors, eye_colors, average_lifespan, \
        language, homeworld)' 'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
      cursor.execute("commit;")

  else:  # Otherwise it will not create database
    print("Database " + DbName + " could not be created!")
    raise SystemExit

if cnx.is_connected():  # when it is connected shows below to the user
  print("Database ", DbName, " is now connected and data inserted.")

  while True:  # These lists are for the user to choose
    options = input(
      "Press 1. List all planets.\n" +
      "Press 2. Search for planet details.\n" +
      "Press 3. Search for species with height higher than given number.\n" +
      "Press 4. What is the most likely desired climate of the given species?\n" +
      "Press 5. What is the average lifespan per species classification?\n" +
      "Press 0. Quit\n"+
      "------------------------------\n"+
      "Choose one of the above: ")

    if options.isnumeric(): # check if the input is a digit.
      options_number = int(options)

    if options_number == 1:  # To show all planets
      planets = cnx.cursor()
      planets.execute("select name from planets;")
      print("--------------------------------------------------")

      for planet in planets:
        print(planet)
      print("--------------------------------------------------")
      input("Hit any key to go to the main menu: ")

    elif options_number == 2:  # search for planet details.
        planetName = input("Enter name of the planet: ")
        planets_detail = cnx.cursor()
        planets_detail.execute("select * from planets where name = " + \
                    "'" + planetName + "'" + ";")
        print("--------------------------------------------------")

        for planet in planets_detail:
          print(planet, end = "")
        print("--------------------------------------------------")
        input("Hit any key to go to the main menu: ")

    elif options_number == 3:  # search for species with height higher than given number.
      height = int(
        input("Enter the average height of the species: "))
      species_height = cnx.cursor()
      species_height.execute(
        "select sp_name,average_height from species where average_height > " + str(height) + ";")
      print("--------------------------------------------------")

      for higher_given_height in species_height:
        print(higher_given_height)
      print("--------------------------------------------------")
      input("Hit any key to go to the main menu")

    elif options_number == 4:  # most likely desired climate of the given species.
      speciesName = input("Enter the name of the species: ")
      nameOfSpecies = cnx.cursor()
      nameOfSpecies.execute(
        "select climate from planets where name in (select homeworld \
          from species where sp_name="+"'"+speciesName+"'"+");")
      print("--------------------------------------------------")

      for desired_climate in nameOfSpecies:
        print(desired_climate)
      print("--------------------------------------------------")
      input("Hit any key to go to the main menu: ")

    elif options_number == 5:  # the average lifespan per species classification.
      species_classification = cnx.cursor()
      species_classification.execute("select classification ,AVG(average_lifespan) \
        from species GROUP BY classification;")
      print("       Species name      |     Average lifespan   ")

      for classification, average_lifespan in species_classification:
        print('%20s' % classification, '%20s' % average_lifespan)
      print("--------------------------------------------------")
      input("Hit any key to go to the main menu: ")

    elif options_number == 0:  # Last conditional statment To end and quit.
      cnx.close()
      break

# DONE