import qrcode
import sys
import json
from pymongo import MongoClient

def create_qr(code):
    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=10, border=4)
    qr.add_data(code)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
    img.save("qr.png")

def create_person(data, connection):
    collection = connection["Certificate"]

    # do not enforce the schema here, more flexibility
    cert =  {
            "PERSON": data,
            "VACCINATION": [],
            "TEST": []
            }

    id = collection.insert_one(cert)
    return id.inserted_id

if __name__ == "__main__":

    inp = sys.argv[1]
    f = open(inp, "r")
    f = json.load(f)

    client = MongoClient("localhost", 27017)

    id = create_person(f, client["Coviddi"])
    print(id)

    create_qr(str(id))