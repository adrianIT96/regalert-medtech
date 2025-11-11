import requests
import pandas as pd

# Konfigurácia pre NCBI E-utility API (PubMed)
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
SEARCH_TERM = "artificial intelligence AND medical device" # Príklad vyhľadávania
RETMAX = 10 # Počet vrátených výsledkov
DB = "pubmed"
RET_TYPE = "json"

def get_pubmed_ids():
    """Získa zoznam PubMed ID (PMID) pre daný vyhľadávací dotaz."""
    params = {
        'db': DB,
        'term': SEARCH_TERM,
        'retmode': RET_TYPE,
        'retmax': RETMAX
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status() # Vyvolá chybu pre 4xx/5xx stavové kódy
        data = response.json()

        # Extrakcia zoznamu ID
        id_list = data['esearchresult']['idlist']
        print(f"Úspešne nájdených {len(id_list)} ID článkov pre RegAlert.")
        return id_list

    except requests.exceptions.RequestException as e:
        print(f"Chyba pri volaní API: {e}")
        return []

if __name__ == "__main__":
    new_ids = get_pubmed_ids()

    # Zatiaľ len vypíšeme ID
    if new_ids:
        print("PMID pre najnovšie články:")
        print(new_ids)