from openpyxl import Workbook
import requests
from bs4 import BeautifulSoup


excel_workbook = Workbook()
excel_worksheet = excel_workbook.active

web_link = 'https://live.skillbox.ru/playlists/code/python'
web_page = requests.get(web_link)
soup = BeautifulSoup(web_page.text, 'html.parser')

for el in soup.find_all(class_='playlist-inner__item'):
    video_name = el.find(class_='playlist-inner-card__title hover-card__text playlist-inner-card__title--big').text
    video_href = 'https://live.skillbox.ru' + el.find(class_='playlist-inner-card hover-card').attrs['href']
    video_duration = el.find(class_='playlist-inner-card__small-info').text.split()[1]
    excel_worksheet.append([video_name, video_duration, video_href])

excel_workbook.save('1.xls')
