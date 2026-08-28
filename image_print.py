import win32ui
import qrcode
from PIL import Image, ImageWin, ImageDraw, ImageFont

imsize = (128, 128)
text_color =(0,0,0)
bkgd_color = (255, 255, 255)  
font_size = 12


def make_qr(text, qr_size = (80,80), save = False, save_path = None):
    qr_img = qrcode.make(text)
    qr_img = qr_img.resize(qr_size)
    
    if save:
        qr_img.save(save_path)
    return(qr_img)


def make_image(qr, text_lines, save_path, image_size = imsize, font_size = font_size, background_color = bkgd_color, text_color = text_color):
    image = Image.new('RGB', image_size, background_color)
    Image.Image.paste(image, qr,(5,1))
    draw = ImageDraw.Draw(image)
    font = ImageFont.truetype("arialbd.ttf", font_size) 
    y = 85
    for l in text_lines:
        draw.text((2, y), l, font=font, fill=text_color)
        y += font_size +5

    image.save(save_path)


def print_label(printer_name, image_file, image_size = imsize):

    hDC = win32ui.CreateDC()
    hDC.CreatePrinterDC(printer_name)

    bmp = Image.open(image_file)
    
    hDC.StartDoc(image_file)
    hDC.StartPage()
    
    dib = ImageWin.Dib(bmp)
    dib.draw(hDC.GetHandleOutput(), (0,0,image_size[0],image_size[1]))
    hDC.EndPage()
    hDC.EndDoc()
    hDC.DeleteDC()