import urllib.request
from bs4 import BeautifulSoup
import pandas as pd

wiki = 'https://pt.wikipedia.org/wiki/Lista_de_capitais_do_Brasil'
page = urllib.request.urlopen(wiki)
soup = BeautifulSoup(page, 'html5lib')
list_item = soup.find_all('span', attrs={'class': 'mw-headline'})

estados = []

for row in list_item:
    name = row.text.strip()
    estados.append(name)

lista_estados = pd.DataFrame(estados, columns=['Estados'])

lista_estados.to_csv('/opt/airflow/scripts/data/estados_br.csv', index=False)