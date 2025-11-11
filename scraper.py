import requests
import pandas as pd
import json
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
# NCBI E-utility API (PubMed) Settings
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

# Database Configuration
DB_FILE = "regalert_data.sqlite"
TABLE_NAME = "pubmed_articles"
# We use SQLite for simplicity in local development
DB_ENGINE = create_engine(f'sqlite:///{DB_FILE}')

# Search Parameters
SEARCH_TERM = "artificial intelligence AND medical device" 
RETMAX = 10 
DB = "pubmed"
ESEARCH_RET_TYPE = "json"


def get_pubmed_ids():
    """Fetches a list of PubMed IDs (PMIDs) based on the search term."""
    print(f"INFO: Starting search for term: '{SEARCH_TERM}'")
    
    params = {
        'db': DB,
        'term': SEARCH_TERM,
        'retmode': ESEARCH_RET_TYPE,
        'retmax': RETMAX
    }

    try:
        response = requests.get(ESEARCH_URL, params=params)
        response.raise_for_status()
        data = response.json()

        id_list = data['esearchresult']['idlist']
        print(f"SUCCESS: Successfully found {len(id_list)} article IDs.")
        return id_list

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed during E-Search API call: {e}")
        return []

def fetch_details_by_id(id_list):
    """Fetches full article details (Title, Date, Journal) based on PMIDs."""
    if not id_list:
        return pd.DataFrame() 

    # Convert the list of IDs into a comma-separated string required by the API
    id_string = ",".join(id_list)
    
    print(f"INFO: Fetching detailed data for {len(id_list)} articles...")
    
    params = {
        'db': DB,
        'id': id_string,
        'retmode': 'json'
    }
    
    try:
        response = requests.get(ESUMMARY_URL, params=params)
        response.raise_for_status()
        summary_data = response.json()
        
        # --- DATA EXTRACTION AND CLEANUP ---
        records = []
        # Get the list of UIDs (Unique Identifiers) from the response
        uids = summary_data.get('result', {}).get('uids', [])
        
        for uid in uids:
            article = summary_data['result'][uid]
            
            # Extracting key data points
            records.append({
                'PMID': uid,
                'Title': article.get('title', 'N/A'),
                'Journal': article.get('fulljournalname', 'N/A'),
                # Using sortdate and slicing to get YYYY/MM/DD format
                'Publication_Date': article.get('sortdate', 'N/A')[:10],
                'Source_URL': f"https://pubmed.ncbi.nlm.nih.gov/{uid}/"
            })
            
        # Convert the list of dictionaries into a Pandas DataFrame
        df = pd.DataFrame(records)
        print("SUCCESS: Data structured into Pandas DataFrame.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed during E-Summary API call: {e}")
        return pd.DataFrame()


def save_to_database(df):
    """
    Saves the processed DataFrame to the SQLite database in append mode.
    """
    if df.empty:
        print("INFO: DataFrame is empty, skipping database save.")
        return
        
    print(f"INFO: Attempting to save {len(df)} records to the database...")

    try:
        # Append new data to the table
        df.to_sql(
            TABLE_NAME, 
            con=DB_ENGINE, 
            if_exists='append', 
            index=False
        )
        
        print(f"SUCCESS: Successfully saved {len(df)} records to {TABLE_NAME} in {DB_FILE}.")

    except Exception as e:
        print(f"ERROR: Failed to save data to database: {e}")


def cleanup_duplicates():
    """Removes duplicate entries based on the PMID column after appending new data."""
    print("INFO: Running database cleanup to remove duplicates...")
    
    # SQL query to delete all rows that are NOT the first occurrence (MIN(rowid)) 
    # for each unique PMID.
    cleanup_sql = text(f"""
        DELETE FROM {TABLE_NAME}
        WHERE rowid NOT IN (
            SELECT MIN(rowid) 
            FROM {TABLE_NAME} 
            GROUP BY PMID
        );
    """)

    try:
        with DB_ENGINE.connect() as connection:
            connection.execute(cleanup_sql)
            connection.commit()
            print("SUCCESS: Duplicate rows cleaned up.")

    except Exception as e:
        print(f"ERROR: Failed during duplicate cleanup: {e}")


def count_records():
    """Counts the total number of unique records in the table."""
    count_sql = text(f"SELECT COUNT(DISTINCT PMID) FROM {TABLE_NAME};")
    
    try:
        with DB_ENGINE.connect() as connection:
            result = connection.execute(count_sql).scalar()
            print(f"DB STATUS: Total unique records after processing: {result}")
            return result
    except Exception as e:
        return 0

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    
    # 1. Get the list of article IDs
    article_ids = get_pubmed_ids()

    # 2. Fetch detailed information and convert to DataFrame
    data_df = pd.DataFrame() 
    if article_ids:
        data_df = fetch_details_by_id(article_ids)
        
    if not data_df.empty:
        
        # Display the head of the DataFrame (optional, for testing)
        print("\n--- FIRST 5 RECORDS IN DATAFRAME ---")
        print(data_df[['PMID', 'Title']].head())

        # 3. Save to database (append mode)
        save_to_database(data_df)
        
        # 4. Remove any duplicates that might have been added
        cleanup_duplicates()
        
    else:
        print("INFO: No new data to save/process.")

    # 5. Final check
    count_records()

    print("\nRegAlert Execution finished.")