import pandas as pd
import numpy as np
import random
import time
import datetime
import pymongo
from pymongo import MongoClient
import dateutil
from dateutil import parser

# PER STAMPARE SU FILE: python generator.py > nomefile.txt
# verrà generato nella cartella in cui si runna

position9 = np.array(['A', 'B', 'C', 'D', 'E', 'H', 'L', 'M', 'P', 'R', 'S', 'T'])

# Doctors
roles = np.array(["Primary", "Intern", "Vaccinator"])

# Institutions
regions = np.array(["Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Fiuli Venezia Giulia",
                    "Lazio", "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia",
                    "Toscana", "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto", "Regno supremo Bavaroff"])
# department issuing the vaccine = "Ambulatory" + random int
type_of_institution = np.array(["Hospital", "Vaccine center", "Pharmacy"])

start_time = datetime.time(00, 00, 00)

doctors = []
institutions = []
people = []


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb+srv://Bavaroff258:mysql@cluster0.9sb6c.mongodb.net/test"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['Coviddi']


class Doctor:

    def __init__(self, data):

        name, surname, mail, telephone, date = data

        # these checks are to make the generator of random names compatibile with the italian id generation rules
        if len(name) < 3:
            name = name + "aaa"
        if len(surname) < 3:
            surname = surname + "aaa"

        self.name = name
        self.surname = surname
        self.birthdate = date
        self.mail = mail
        self.role = "medico"
        self.telephone = telephone

        cf = name[0] + name[1] + name[2] + surname[0] + surname[1] + surname[2] + date[2] + date[3] + random.choice(
            position9) + date[5] + date[6] + random.choice(position9) + str(random.randint(100, 999)) + random.choice(
            position9)

        self.cf = cf.upper()


class Institution:
    def __init__(self, data):
        name, latitude, longitude = data

        self.name = name
        self.coordinates = str(latitude) + " " + str(longitude)
        self.type = random.choice(type_of_institution)
        self.department = "Clinic n." + str(random.randint(1, 999))
        self.region = random.choice(regions)


class Person:

    def __init__(self, data):

        name, surname, birthday, sex, address, telephone, mail = data

        # these checks are to make the generator of random names compatibile with the italian id generation rules
        if len(name) < 3:
            name = name + "aaa"
        if len(surname) < 3:
            surname = surname + "aaa"

        self.name = name
        self.surname = surname
        self.birthdate = birthday
        self.sex = sex
        self.mail = mail
        self.address = address
        self.telephone = telephone

        cf = name[0] + name[1] + name[2] + surname[0] + surname[1] + surname[2] + birthday[2] + birthday[
            3] + random.choice(
            position9) + birthday[5] + birthday[6] + random.choice(position9) + str(
            random.randint(100, 999)) + random.choice(
            position9)

        self.cf = cf.upper()


def randomize_and_cut(df, quantity):
    return df.sample(frac=1).reset_index(drop=True).head(quantity)


def generate_doctors(df, quantity, db):
    df = randomize_and_cut(df, quantity)
    ids = []

    doctors_coll = db["Doctors"]

    for i in range(quantity):
        d = Doctor(df.iloc[[i]].values[0])
        ids.append(d.cf)
        doctors.append(d)

        item = {
            "CF": d.cf,
            "Name": d.name,
            "Surname": d.surname,
            "Phone_number": d.telephone,
            "Mail": d.mail,
            "Role": d.role
        }
        doctors_coll.insert_one(item)

    df["id"] = ids  # added the CF as column, but it's not the id

    return df


def generate_institutions(df, quantity, db):
    df = randomize_and_cut(df, quantity)

    institutions_coll = db["Institutions"]

    for i in range(quantity):
        ins = Institution(df.iloc[[i]].values[0])
        institutions.append(ins)

        item = {
            "Name": ins.name,
            "Coordinates": ins.coordinates,
            "Type": ins.type,
            "Department": ins.department,
            "Region": ins.region
        }
        institutions_coll.insert_one(item)


def generate_people(df, quantity):
    df = randomize_and_cut(df, quantity)
    ids = []

    for i in range(quantity):
        p = Person(df.iloc[[i]].values[0])
        ids.append(p.cf)
        people.append(p)

    df["id"] = ids  # added the CF as column, but it's not the id

    return df


def generate_certificate(quantity, db):
    centificate_coll = db["Certificate"]

    # capire come gestire la roba dei vettori come si deve
    for i in range(quantity):
        pers = random.choice(people)

        randoc = random.choice(doctors)
        doc1 = dbname.Doctors.find_one({"CF": randoc.cf}, {"_id": 1})
        randinst = random.choice(institutions)
        inst1 = dbname.Institutions.find_one({"Name": randinst.name}, {"_id": 1})

        item = {
            "Person": {
                "CF": pers.cf,
                "Name": pers.name,
                "Surname": pers.surname,
                "Birthday": pers.birthdate,
                "Sex": pers.sex,
                "Address": pers.address,
                "Phone number": pers.telephone,
                "Email": pers.mail,
                "Emergency contact": {
                    "Phone number": pers.telephone,
                    "Details": "Gli piace la nutella"
                }
            },
            "Vaccination": {
                "Date": "",
                "Place": "Hub n." + str(random.randint(1, 1000)),
                "Valid": str(random.choice([True, False])),

                "Vaccine": {
                    "Pharma": str(random.choice(["Pfizer", "Astrazeneca", "Moderna", "J&J"])),
                    "Type": "mRNA",
                    "Batch": str(random.randint(1, 1000000)),
                    "Production date": ""
                },
                "Doctor": doc1,
                "Institution": inst1
            },
            "Test": {
                "Place": "Test Center n." + str(random.randint(1, 1000)),
                "Date": "",
                "Result": random.choice(["Positive", "Negative"]),
                "valid": random.choice(["Valid", "Invalid"]),
                "Doctor": doc1,
                "Institution": inst1
            }

        }
        centificate_coll.insert_one(item)


def generator(dfDoctors, dfInstitutions, dfPeople, db):
    generate_doctors(dfDoctors, 10, db)
    generate_institutions(dfInstitutions, 10, db)
    dfPeople = generate_people(dfPeople, 100)
    generate_certificate(150,db)


# ------------------------------------MAIN-----------------------------------
df_doctors = pd.read_csv(r'doctors.csv')
df_institutions = pd.read_csv(r'institutions.csv')
df_people = pd.read_csv(r'people.csv')
dbname = get_database()

generator(df_doctors, df_institutions, df_people, dbname)

