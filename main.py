import pandas as pd
import numpy as np
import random
from random import randrange
import time
import datetime
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient
import sys

local_mode = False
if len(sys.argv) > 1:
    if sys.argv[1] == "-local":
        local_mode = True

position9 = np.array(['A', 'B', 'C', 'D', 'E', 'H', 'L', 'M', 'P', 'R', 'S', 'T'])

# Doctors
roles = np.array(["Primary", "Intern", "Assistant"])

# Institutions
regions = np.array(["Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Fiuli Venezia Giulia",
                    "Lazio", "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia",
                    "Toscana", "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"])

type_of_institution = np.array(["Hospital", "Vaccine center", "Pharmacy"])

doctors = []
institutions = []
people = []

start_date = datetime.strptime('2020-2-20T00:00:00.000+00:00', '%Y-%m-%dT%H:%M:%S.000+00:00')
end_date = datetime.strptime('2021-12-14T00:00:00.000+00:00', '%Y-%m-%dT%H:%M:%S.000+00:00')

# unique for all vaccine
production_date = datetime.strptime('2020-01-14T00:00:00.000+00:00', '%Y-%m-%dT%H:%M:%S.000+00:00')

unvaccinated = 0
probability_of_positive = 0.1


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    connection_string = "mongodb+srv://marc:mysql@claster.lecz1.mongodb.net/test"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    if local_mode:
        client = MongoClient("localhost", 27017)
    else:
        client = MongoClient(connection_string)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['Coviddi2']


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


def working_doctors(db):
    n_doctors = random.randint(1, 3)
    vacc_doctors = []
    for i in range(n_doctors):
        randoc = random.choice(doctors)
        doc = db.Doctors.find_one({"cf": randoc.cf}, {"_id": 1})
        vacc_doctors.append(doc["_id"])

    return vacc_doctors


def coll_person(pers):
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
            "details": "Null"
        }
    }
    return person


def coll_vacc(db):
    n_doses = random.randint(0, 3)
    vaccinations = []

    if n_doses == 0:
        unvaccinated = 1
        return vaccinations

    unvaccinated = 0
    date = random_date2(start_date, end_date)

    randinst = random.choice(institutions)
    inst = db.Institutions.find_one({"name": randinst.name}, {"_id": 1})
    inst = inst["_id"]

    vaccination = {
        "date_performed": date,
        #"duration": 1,
        "expiration_date": date + timedelta(days=30),
        "place": "Hub n." + str(random.randint(1, 1000)),
        "valid": bool(1),

        "VACCINE": {
            "pharma": random.choice(["Pfizer", "Astrazeneca", "Moderna", "J&J"]),
            "type": random.choice(["mRNA", "viral vector"]),
            "batch": str(random.randint(1, 1000000)),
            "production_date": production_date
        },
        "Doctor": working_doctors(db),
        "Institution": inst
    }
    vaccinations.append(vaccination)

    # ottimizzabile
    nextdate = date + timedelta(days=30)
    for i in range(n_doses - 1):
        randinst = random.choice(institutions)
        inst = db.Institutions.find_one({"name": randinst.name}, {"_id": 1})
        inst = inst["_id"]

        vaccination2 = {
            "date_performed": nextdate,
            #"duration": 6,
            "expiration_date": date + timedelta(days=180),
            "place": "Hub n." + str(random.randint(1, 1000)),
            "valid": bool(random.randint(0, 1)),

            "VACCINE": {
                "pharma": random.choice(["Pfizer", "Astrazeneca", "Moderna", "J&J"]),
                "type": random.choice(["mRNA", "viral vector"]),
                "batch": str(random.randint(1, 1000000)),
                "production_date": production_date
            },
            "Doctor": working_doctors(db),
            "Institution": inst
        }
        #next dose at most 20 days after the expiration date of the previous one
        nextdate = nextdate + timedelta(days=random.randint(170, 200))
        vaccinations.append(vaccination2)

    return vaccinations


# unvaccinated vale 1 se non è vaccinato e lo sommo al numero di test cpsì che ce ne sia almeno 1
def coll_test(db):
    n_tests = random.randint(0, 10) + unvaccinated
    tests = []

    date = random_date2(start_date, end_date)

    for i in range(n_tests):
        #date of the next test
        date = date + timedelta(days=random.randint(2, 100))
        randinst = random.choice(institutions)
        inst = db.Institutions.find_one({"name": randinst.name}, {"_id": 1})
        inst = inst["_id"]

        if (random.uniform(0, 1) > probability_of_positive):
            result = "negative"
        else:
            result = "positive"

        test = {
            "place": "Test Center n." + str(random.randint(1, 1000)),
            "date_performed": date,
            #"duration": 2,
            "expiration_date": date + timedelta(days=2),
            "result": result,  # non mettere 0,5% ma tipo 0,1
            "valid": bool(random.randint(0, 1)),
            "Doctor": working_doctors(db),
            "Institution": inst
        }
        tests.append(test)

    return tests


def generate_certificate(quantity, db):
    centificate_coll = db["Certificate"]

    centificate_coll.drop()

    # for each person in the database
    for i in range(quantity):
        # pers = random.choice(people)

        item = {
            "PERSON": coll_person(people[i]),
            "VACCINATION": coll_vacc(db),
            "TEST": coll_test(db)
        }
        centificate_coll.insert_one(item)


def generator(dfDoctors, dfInstitutions, dfPeople, db):
    n_people = 500          # max 1000
    n_doctors = 20          # max 50
    n_institutions = 40     # max 50

    generate_doctors(dfDoctors, n_doctors, db)
    generate_institutions(dfInstitutions, n_institutions, db)
    dfPeople = generate_people(dfPeople, n_people)
    generate_certificate(n_people, db)


# ------------------------------------MAIN-----------------------------------
df_doctors = pd.read_csv(r'doctors.csv')
df_institutions = pd.read_csv(r'institutions.csv')
df_people = pd.read_csv(r'people.csv')
dbname = get_database()

generator(df_doctors, df_institutions, df_people, dbname)
