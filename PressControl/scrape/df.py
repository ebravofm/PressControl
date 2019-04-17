import pickle
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool
import numpy
import datetime

def main():
    tprint('[+] Encendido: Recuperando links de DiarioFinanciero')
    pags = list(range(1,2400))

    links = []

    chunks = numpy.array_split(pags, 100)
    n = 1
    for chunk in chunks:
        tprint('[·] Procesando Chunk '+str(n)+' de '+str(len(chunks)))
        p = Pool(24)  # Pool tells how many at a time
        records = p.map(news_from_page, pags)
        p.terminate()
        p.join()
        tprint('[+] '+str(len(records))+' noticias recuperadas.')
        for r in records:
            links += r
        n += 1
    
        with open('df.pkl', 'wb') as f:
            pickle.dump(links, f)

    tprint('[+] LISTO. Guardado en df.pkl')


def news_from_page(pag):
    links = []
    url = 'https://www.df.cl/cgi-bin/prontus_search.cgi?search_prontus=noticias&search_idx=all&search_tmp=search.html&search_form=yes&search_pag='+str(pag)+'&search_resxpag=30&search_maxpags=82&search_orden=cro&search_meta1=&search_meta2=&search_meta3=&search_seccion=&search_tema=&search_subtema=&search_fechaini=&search_fechafin=&search_texto=si&search_modo=and&search_comodines=no&vista='

    page = requests.get(url)
    html = bs(page.content, 'lxml')
    box = html.find('div', {'id': 'wrap-noticias'})
    articles = box.find_all('article')

    for art in articles:
        links += [art.h2.a['href']]
        
    return links
        

def tprint(*args):
    stamp = '[{:%d/%m-%H:%M}]'.format(datetime.datetime.now())
    print(stamp, *args)

    
if __name__ == "__main__":
    main()
