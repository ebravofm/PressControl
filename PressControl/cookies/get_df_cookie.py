
#### GET COOKIE TO PICKLE
#### NO PROBADO EN SERVIDOR; SOLO LAPTOP

from selenium import webdriver
from time import sleep
from selenium.webdriver.common.keys import Keys
import pickle

d = webdriver.Firefox()
d.get('https://df.cl')

sleep(11)

ingreso = d.find_element_by_id('dffull')
ingreso.click()

email = d.find_element_by_name('cont_email')
email.send_keys('fordonez@fen.uchile.cl')
passwd = d.find_element_by_name('password')
passwd.send_keys('dcba4321')
passwd.send_keys(Keys.ENTER)

sleep(5)

df_cookies = d.get_cookies()
d.close()

d ={}
for cookie in df_cookies:
    d[cookie['name']] = cookie['value']

with open('/Users/emilio/Downloads/df_cookies.pkl', 'wb') as f:
    pickle.dump(d, f)
    
#### PROBAR

import requests

url = 'http://df.cl/noticias/economia-y-politica/actualidad/kast-se-abre-a-opcion-de-ir-directo-a-la-eleccion-de-noviembre-si-bloque/2017-04-11/205517.html'
page = requests.get(url, cookies=d)

from newsplease import NewsPlease
art = NewsPlease.from_html(page.content)
