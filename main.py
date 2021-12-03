import pandas as pd
import numpy as np
import random
from random import randrange
import time
import datetime
from datetime import timedelta
from datetime import datetime
from pymongo import MongoClient
import sys


local_mode = False
if len(sys.argv) > 1:
    if sys.argv[1] == "-local":
        local_mode = True

position9 = np.array(['A', 'B', 'C', 'D', 'E', 'H', 'L', 'M', 'P', 'R', 'S', 'T'])

# Doctors
roles = np.array(["Primary", "Intern", "Vaccinator"])

# Institutions
regions = np.array(["Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Fiuli Venezia Giulia",
                    "Lazio", "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia",
                    "Toscana", "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto", "Regno supremo Bavaroff"])
# department issuing the vaccine = "Ambulatory" + random int
type_of_institution = np.array(["Hospital", "Vaccine center", "Pharmacy"])

# start_time = datetime.time(00, 00, 00)

doctors = []
institutions = []
people = []

start_date = datetime.strptime('2020-2-20T00:00:00.000+00:00', '%Y-%m-%dT%H:%M:%S.000+00:00')
end_date = datetime.strptime('2021-12-14T00:00:00.000+00:00', '%Y-%m-%dT%H:%M:%S.000+00:00')
# unique for all vaccine
production_date = datetime.strptime('2020-01-14T00:00:00.000+00:00', '%Y-%m-%dT%H:%M:%S.000+00:00')


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    connection_string = "mongodb+srv://marc:mysql@claster.lecz1.mongodb.net/test"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    if local_mode:
        client = MongoClient("localhost", 27017)
    else:
        client = MongoClient(connection_string)

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


def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%dT%H:%M:%S.000+00:00', prop)


def random_date2(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def generate_doctors(df, quantity, db):
    df = randomize_and_cut(df, quantity)
    ids = []

    doctors_coll = db["Doctors"]

    doctors_coll.drop()

    for i in range(quantity):
        d = Doctor(df.iloc[[i]].values[0])
        ids.append(d.cf)
        doctors.append(d)

        item = {
            "cf": d.cf,
            "name": d.name,
            "surname": d.surname,
            "phone_number": d.telephone,
            "mail": d.mail,
            "role": d.role
        }
        doctors_coll.insert_one(item)

    df["id"] = ids  # added the CF as column, but it's not the id

    return df


def generate_institutions(df, quantity, db):
    df = randomize_and_cut(df, quantity)

    institutions_coll = db["Institutions"]

    institutions_coll.drop()

    for i in range(quantity):
        ins = Institution(df.iloc[[i]].values[0])
        institutions.append(ins)

        item = {
            "name": ins.name,
            "coordinates": ins.coordinates,
            "type": ins.type,
            "department": ins.department,
            "region": ins.region
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

    centificate_coll.drop()

    # capire come gestire la roba dei vettori come si deve
    for i in range(quantity):
        pers = random.choice(people)

        randoc = random.choice(doctors)
        doc1 = db.Doctors.find_one({"cf": randoc.cf}, {"_id": 1})
        randoc = random.choice(doctors)
        doc2 = db.Doctors.find_one({"cf": randoc.cf}, {"_id": 1})
        randinst = random.choice(institutions)
        inst1 = db.Institutions.find_one({"name": randinst.name}, {"_id": 1})

        date = random_date2(start_date, end_date)
        date2 = random_date2(start_date, end_date)

        ddd = [doc1, doc2]

        person = {
            "cf": pers.cf,
            "name": pers.name,
            "surname": pers.surname,
            "birthdate": pers.birthdate,
            "sex": pers.sex,
            "address": pers.address,
            "phone_number": pers.telephone,
            "email": pers.mail,
            "EMERGENCY_CONTACT": {
                "phone_number": pers.telephone,
                "details": "Gli piace la nutella"
            }
        }
        vaccination = {
            "date_performed": date,
            "duration": 1,
            "place": "Hub n." + str(random.randint(1, 1000)),
            "valid": bool(random.choice(["true", "false"])),

            "VACCINE": {
                "pharma": str(random.choice(["Pfizer", "Astrazeneca", "Moderna", "J&J"])),
                "type": "mRNA",
                "batch": str(random.randint(1, 1000000)),
                "production_date": production_date
            },
            "Doctor": ddd,
            "Institution": inst1
        }

        test = {
            "place": "Test Center n." + str(random.randint(1, 1000)),
            "date_performed": date2,
            "duration": 2,
            "result": random.choice(["positive", "negative"]),  # non mettere 0,5% ma tipo 0,1
            "valid": bool(random.choice(["true", "false"])),
            "Doctor": doc1,
            "Institution": inst1
        }

        item = {
            "PERSON": person,
            "VACCINATION": vaccination,
            "TEST": test
        }
        centificate_coll.insert_one(item)


def generator(dfDoctors, dfInstitutions, dfPeople, db):
    generate_doctors(dfDoctors, 10, db)
    generate_institutions(dfInstitutions, 10, db)
    dfPeople = generate_people(dfPeople, 100)
    generate_certificate(150, db)


# ------------------------------------MAIN-----------------------------------
df_doctors = pd.read_csv(r'doctors.csv')
df_institutions = pd.read_csv(r'institutions.csv')
df_people = pd.read_csv(r'people.csv')
dbname = get_database()

generator(df_doctors, df_institutions, df_people, dbname)

'''
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
                "Date": date,
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
                "Date": date2,
                "Result": random.choice(["Positive", "Negative"]),  # non mettere 0,5% ma tipo 0,1
                "valid": random.choice(["Valid", "Invalid"]),
                "Doctor": doc1,
                "Institution": inst1
            }

        }'''
