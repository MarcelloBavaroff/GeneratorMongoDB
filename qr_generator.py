import qrcode
from PIL import Image
import sys

def create_qr(code):
    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=10, border=4)
    qr.add_data(code)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert('RGB')
    img.save("qr_"+code+".png")

if __name__ == "__main__":
    inp = sys.argv[1]
    create_qr(inp)