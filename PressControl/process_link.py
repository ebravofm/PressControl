import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs
from PressControl.utils import read_config, tprint
import pandas as pd
import random
import urllib
from newsplease import NewsPlease
from urllib.parse import urlparse
import datetime
from googlesearch import search


def process_link(link):

    link = get_direct_link(link)
    
    if link != '':
        try:
            d = process_outer(link)
            
        except requests.exceptions.ConnectionError:
            error = '[-] Connection Error '+link
            tprint(error, important=False)
            d = {'error': 1, 'info': 'ConnectionError'}
            
        except Exception as exc:
            error = '[-] Error General '+link+' :' + str(exc)[:100]
            tprint(error, important=False)
            d = {'error': 1, 'info': error}
            
    else:
        # Mark for deletion if tweet does not contain any links.
        error = '[-] Link Vacío en process_link'
        tprint(error, important=False)
        d = {'error': 1, 'borrar': 1, 'info': error}

    return d


def get_direct_link(twitter_link):
    '''
    Extract direct link from tweet.
    '''
    
    if 'twitter' in twitter_link:
        indirect_links = []

        try:
            
            page = requests.get(twitter_link)
            lxml = bs(page.content, 'lxml')
            box = lxml.find_all('p', {"class": "TweetTextSize--jumbo"})[0]
            links = [l for l in box.find_all('a') if 'pic.twitter' not in l]
            
            for l in links:
                
                try:
                    indirect_links.append(l['data-expanded-url'])
                except Exception as exc:
                    pass
                
            if not links:
                
                try:
                    indirect_links += [x for x in box.text.split() if 'http' in x]
                except:
                    pass
                    
        except Exception as exc:
            
            pass
            #tprint(str(exc)[:60], important=False)

        return indirect_links[0] if len(indirect_links)!=0 else ''
    
    else:
        
        return twitter_link
    

def process_inner(requests_page):
    art = NewsPlease.from_html(requests_page.content)
    d = {}
    d['titulo'] = art.title
    d['bajada'] = art.description
    d['contenido'] = art.text
    d['autor'] = ', '.join(art.authors)
    d['fecha'] = art.date_publish
    d['link'] = requests_page.url
    d['fuente'] = urlparse(requests_page.url).netloc.split('.')[-2].capitalize()
    d['imagen'] = art.image_url
    d['error'] = 0
    try:
        d['ano'] = art.date_publish.year
    except: 
        pass
    
    
    return d


def process_outer(link):

    page = requests.get(link)
    fuente = urlparse(page.url).netloc.split('.')[-2].capitalize()

    # Personalized treatment by source

    action = {
        'Df': process_Df,
        'Emol': process_Emol,
        'Latercera': process_Latercera,
        'Cooperativa': process_Cooperativa,
        'Elmostrador': process_Elmostrador,
        'Biobiochile': process_Biobiochile,
    }
    if fuente in action.keys():
        d = action[fuente](page)
    else:
        d = process_inner(page)


    # Add year

    try:
        d['ano'] = d['fecha'].year
    except:
        pass


    # Mark for deletion if link isn't from any source in config.txt.
    try:
        if d['fuente'] not in read_config()['sources']:
            d['borrar'] = 1
            d['info'] = 'Borrar, no pertenece a los dominios buscados.'
    except:
        pass
    
    # Restrict text field to 50.000 characters
    try:
        d['contenido'] = d['contenido'][:50000]
    except:
        pass

    # Encode Links
    try:
        d['imagen'] = urllib.parse.quote(d['imagen']).replace('%3A', ':')
    except:
        pass
    try:
        d['link'] = urllib.parse.quote(d['link']).replace('%3A', ':')
    except:
        pass

    # Encode content and bajada and titulo
    try:
        d['contenido'] = d['contenido'].encode('latin1', 'ignore').decode('latin1')
        #d['contenido'] = d['contenido'].replace('\x80', '')
    except:
        pass

    try:
        d['bajada'] = d['bajada'].encode('latin1', 'ignore').decode('latin1')
        d['bajada'] = d['bajada'].replace('\x80', '')
    except:
        pass   

    try:
        d['titulo'] = d['titulo'].encode('latin1', 'ignore').decode('latin1')
        d['titulo'] = d['titulo'].replace('\x80', '')
    except:
        pass
    
    try:
        if d['titulo']==None and d['contenido']==None:
            d['error'] = 1
            d['borrar'] = 1
            d['info'] = 'Title and content blank'
    except:
        pass
        
        
    return d




# Add aditional steps by source:


def process_Df(page):
    page = requests.get(page.url, cookies=pd.read_pickle('cookies/df.pkl'))
    d = process_inner(page)
    soup = bs(page.content, 'lxml')
    try:
        d['seccion'] = soup.find('meta', {'name': 'keywords'})['content'].strip()
    except Exception as exc:
        tprint('[-] Error parsing seccion (Df) - ', exc, important=False)
    try:
        d['contenido'] = '\n'.join([p for p in d['contenido'].split('\n') if len(p.split())>4 and p!=d['bajada']])
    except Exception as exc:
        tprint('[-] Error parsing contenido (Df) - ', exc, important=False)
    return d


def process_Emol(page):
    d = process_inner(page)
    soup = bs(page.content, 'lxml')
    try:
        d['seccion'] = d['link'].split('/')[4].capitalize()
    except Exception as exc:
        tprint('[-] Error parsing seccion (Emol) - ', exc, important=False)

    try:
        d['autor'] = soup.find('div', {'class', 'info-notaemol-porfecha'}).text.split('|')[-1].strip().replace('Por ','').replace('Redactado por ', '')
    except Exception as exc:
        tprint('[-] Error parsing seccion (Emol) - ', exc, important=False)

    return d


def process_Latercera(page):
    
    d = {}
    
    if 'Lo sentimos, estamos actualizando el sitio' not in page.text:
        d = process_inner(page)
                
    else:
        ### Buscar noticia en google, si es necesario.
        scraped_link = page.url.strip('/')
        tprint('[-] Link Latercera no encontrado', page.url, important=False)
        
        new_link = 'https://www.latercera.com/noticia/'+'-'.join([p for p in scraped_link.split('/')[-1].split('.')[0].split('-') if not p.isdigit()])
        #print(new_link)
        page = requests.get(new_link)
        
        if 'Lo sentimos, estamos actualizando el sitio' not in page.text:
            d = process_inner(page)
            tprint('[+] Link Latercera encontrado (intento:1): ', new_link, important=False)
        
        else:
            try:
                tprint('[·] Google Searching...', important=False)
                buscar = ' '.join([p for p in scraped_link.split('/')[-1].split('.')[0].split('-') if not p.isdigit()]) + ' site:latercera.com'
                results = search(buscar, stop=5)
                rs = []
                for r in results:
                    rs.append(r)
                result = [r for r in rs if 'sitemap' not in r][0]

                if 'sitemap' not in result:
                    tprint('[+] Resultado en Google (intento:2):',result, important=False)
                    page = requests.get(result)
                    d = process_inner(page)
                else:

                    d['error'] = 1
                    d['info'] = 'Link Latercera no encontrado en google'
                    
            except Exception as exc:
                tprint('[-] Link Latercera no encontrado', important=False)
                d['error'] = 1
                d['info'] = 'Link Latercera no encontrado en google'

    soup = bs(page.content, 'lxml')
    
    ### Recuperar Imagen.
    try:
        d['imagen'] = soup.find('figure').find('img')['src']
    except Exception as exc:
        tprint('[-] Error parsing imagen (Latercera) - ', exc, important=False)

    ### Recuperar Autor
    
    try:
        d['autor'] = [h.text for h in soup.find_all('h4') if 'Autor' in h.text][0].replace('Autor: ', '')
    except Exception as exc:
        tprint('[-] Error parsing autor (Latercera) - ', exc, important=False)
        

    try:
        if d['bajada'] == None:
            d['bajada'] = soup.find('div', {'class': 'bajada-art'}).text
    except Exception as exc:
        tprint('[-] Error parsing bajada (Latercera) - ', exc, important=False)

    try:
        if d['fecha'] == None:
            fecha = ' '.join(soup.find('span', {'class': 'time-ago'}).text.replace('|', '').split())
            d['fecha'] = datetime.datetime.strptime(fecha, '%d/%m/%Y %I:%M %p')
    except Exception as exc:
        tprint('[-] Error parsing date (Latercera) - ', exc, important=False)

    try:
        d['seccion'] = soup.find('meta', property='article:section')['content']
    except:
        try:
            d['seccion'] = [x.find('a').text for x in soup.find_all('h4') if x.find('a')!=None and 'canal' in x.find('a')['href']][0]
        except Exception as exc:
            tprint('[-] Error parsing section (Latercera) - ', exc, important=False)


    d['tags'] = ', '.join([x['content'] for x in soup.find_all('meta', property='article:tag')])
    if not d['tags']:
        try:
            d['tags'] = ', '.join([x.text for x in soup.find('div', {'class': 'tags-interior'}).find_all('a')])
        except Exception as exc:
            tprint('[-] Error parsing tags (Latercera) - ', exc, important=False)
    
    return d


def process_Cooperativa(page):
    d = process_inner(page)
    try:
        if 'al aire libre' in d['titulo'].lower():
            d = {'borrar':1, info:'Borrar, Al aire libre'}
    except:
        pass
    
    soup = bs(page.content, 'lxml')

    try:
        d['autor'] = soup.find('div', {'class': 'fecha-publicacion'}).find('span').text
    except Exception as exc:
        tprint('[-] Error parsing autor (Cooperativa) - ', exc, important=False)

    try:
        d['seccion'] = soup.find('a', {'id': 'linkactivo'}).text
    except Exception as exc:
        tprint('[-] Error parsing seccion (Cooperativa) - ', exc, important=False)

    try:
        d['tags'] = soup.find('meta', {'name': 'keywords'})['content'].strip()
    except Exception as exc:
        tprint('[-] Error parsing tags (Cooperativa) - ', exc, important=False)

    try:
        d['link'] = soup.find('meta', property='og:url')['content']
    except Exception as exc:
        tprint('[-] Error parsing link (Cooperativa) - ', exc, important=False)
    
    if not d['fecha']:
        try:
            fecha = [x for x in d['link'].split('/') if '-' in x][-1].split('-')
            d['fecha'] = datetime.datetime(*map(int,fecha))
        except Exception as exc:
            tprint('[-] Error parsing fecha (Cooperativa) - ', exc, important=False)
    
    try:
        if 'www.cooperativa.cl' not in d['imagen'] and d['imagen']:
            d['imagen'] = 'https://www.cooperativa.cl'+d['imagen']
    except Exception as exc:
        tprint('[-] Error fixing imagen (Cooperativa) - ', exc, important=False)

    return d


def process_Elmostrador(page):
    d = process_inner(page)
    soup = bs(page.content, 'lxml')
    d['bajada'] = None
    try:
        d['bajada'] = soup.find('figcaption').text
    except Exception as exc:
        tprint('[-] Error parsing bajada (Elmostrador) - ',exc, important=False)

    try:
        d['autor'] = soup.find('p', {'class': 'autor-y-fecha'}).find('a').text
    except Exception as exc:
        tprint('[-] Error parsing autor (Elmostrador) - ',exc, important=False)

    try:
        if 'www.elmostrador.cl' not in d['imagen'] and d['imagen']:
            d['imagen'] = 'https://www.elmostrador.cl'+d['imagen']
    except Exception as exc:
        tprint('[-] Error fixing image (Elmostrador) - ',exc, important=False)

    if not d['fecha']:
        try:
            fecha = [s for s in d['link'].split('/') if s.isdigit()][:3]
            d['fecha'] = datetime.datetime(*map(int,fecha))
        except Exception as exc:
            tprint('[-] Error parsing fecha (Elmostrador) - ',exc, important=False)
    
    try:
        d['seccion'] = ' '.join([x for x in soup.find_all('h2') if x.find('i')!=None][0].text.split())
    except Exception as exc:
        tprint('[-] Error parsing seccion (Elmostrador) - ',exc, important=False)
    
    try:
        d['contenido'] = d['contenido'].split('__________________')[0]
    except Exception as exc:
        tprint('[-] Error fixing contenido (Elmostrador) - ',exc, important=False)
    
    return d


def process_Biobiochile(page):
    d = process_inner(page)
    soup = bs(page.content, 'lxml')
    try:
        d['autor'] = soup.find('div', {'class': 'nota-autor'}).find('a').text
    except Exception as exc:
        tprint('[-] Error parsing autor (Biobiochile) - ',exc, important=False)
    try:
        d['seccion'] = ' '.join(soup.find('div', {'class': 'categoria-titulo-nota'}).text.split())
    except Exception as exc:
        tprint('[-] Error parsing seccion (Biobiochile) - ',exc, important=False)
    try:
        d['contenido'] = soup.find('div', {'class': 'nota-contenido'}).text
        d['contenido'] = d['contenido'].replace('Etiquetas de esta nota:', '')
    except Exception as exc:
        tprint('[-] Error parsing contenido (Biobiochile) - ',exc, important=False)
    try:
        d['bajada'] = None
    except Exception as exc:
        tprint('[-] Error parsing bajada (Biobiochile) - ',exc, important=False)
        
    return d

