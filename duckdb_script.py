#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import duckdb    # Libreria per DuckDB, un database SQL veloce in-memory
import time      # Per misurare i tempi di esecuzione
import os        # Per operazioni sui file
import csv       # Per la lettura dei file CSV
import logging   # Per i log delle operazioni
from dotenv import load_dotenv  # Per caricare variabili da .env

# Carica le variabili d'ambiente dal file .env
# Questo ci permette di configurare lo script senza modificare il codice
load_dotenv()

# Configurazione del logging
# Ho scelto questo formato perché mostra il timestamp preciso, utile per debug
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Percorso del file CSV e database DuckDB
# Se non definiti nel .env, usiamo valori predefiniti
CSV_FILE = os.getenv('CSV_FILE_2000', 'esercizi/input_2000.csv')
DB_FILE = os.getenv('DUCKDB_DB_FILE', 'utenti.duckdb')

def create_connection():
    """Crea una connessione al database DuckDB.
    
    DuckDB è un database analitico molto veloce, perfetto per questo tipo
    di esercizio. A differenza di SQLite, è ottimizzato per query analitiche.
    """
    try:
        logger.info(f"Connessione al database DuckDB {DB_FILE}...")
        conn = duckdb.connect(DB_FILE)
        logger.info("Connessione stabilita con successo.")
        return conn
    except Exception as e:
        logger.error(f"Errore durante la connessione al database: {e}")
        raise

def get_csv_header(csv_file):
    """Ottiene l'intestazione dal file CSV.
    
    È importante conoscere la struttura del CSV prima di caricarlo,
    così possiamo capire quali colonne ci sono e come si chiamano.
    """
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)  # Leggiamo solo la prima riga
            return header
    except Exception as e:
        logger.error(f"Errore durante la lettura dell'intestazione CSV: {e}")
        raise

def load_csv_to_database(conn, csv_file):
    """Carica i dati dal CSV nel database DuckDB.
    
    PASSAGGIO CHIAVE: La vera potenza di DuckDB è qui. A differenza di altri DB,
    DuckDB può leggere e analizzare direttamente i CSV senza necessità di 
    caricarli riga per riga, il che lo rende estremamente veloce per questi task.
    """
    try:
        logger.info(f"Caricamento dati dal file {csv_file}...")
        
        # OTTIMIZZAZIONE IMPORTANTE: DuckDB può creare una vista diretta sul CSV!
        # Questo è molto più efficiente di un caricamento manuale riga per riga
        conn.execute(f"""
            CREATE OR REPLACE VIEW csv_data AS
            SELECT * FROM read_csv_auto('{csv_file}', header=true);
        """)
        
        # Contiamo quante righe ci sono nel CSV
        result = conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()
        total_rows = result[0]
        logger.info(f"File CSV letto con {total_rows} righe")
        
        # Vediamo quali colonne ha rilevato DuckDB
        # DuckDB è intelligente nell'inferire i tipi di dato automaticamente
        table_info = conn.execute("PRAGMA table_info(csv_data)").fetchall()
        logger.info(f"La vista CSV ha {len(table_info)} colonne")
        
        # TECNICA DI CARICAMENTO: Invece di caricare ogni riga, possiamo
        # semplicemente creare una tabella dal contenuto della vista
        # Questo è estremamente più veloce di un inserimento riga per riga
        conn.execute("DROP TABLE IF EXISTS utenti")
        conn.execute("CREATE TABLE utenti AS SELECT * FROM csv_data")
        
        # Verifichiamo quante righe sono state inserite
        result = conn.execute("SELECT COUNT(*) FROM utenti").fetchone()
        total_rows = result[0]
        
        logger.info(f"Caricamento completato: {total_rows} righe inserite nella tabella utenti.")
        return total_rows
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati: {e}")
        raise

def count_info_domain_websites(conn):
    """Conta quanti link in Website sono del dominio di primo livello 'info'
    
    PARTE CRUCIALE: questa funzione implementa la query richiesta nell'esercizio.
    Ho scritto codice robusto che funziona anche se la colonna non si chiama
    esattamente 'website' ma qualcosa di simile.
    """
    try:
        logger.info("Esecuzione query per contare website con dominio di primo livello 'info'...")
        
        # ROBUSTEZZA: Prima troviamo quale colonna contiene i siti web
        # Questo rende lo script più flessibile rispetto a variazioni nel CSV
        table_info = conn.execute("PRAGMA table_info(utenti)").fetchall()
        website_col = None
        
        # Cerchiamo colonne che potrebbero contenere URL di siti web
        for col in table_info:
            col_name = col[1].lower()
            if col_name == 'website' or 'site' in col_name or 'sito' in col_name:
                website_col = col[1]
                break
        
        # Se non troviamo colonne adatte, non possiamo procedere
        if not website_col:
            logger.error("Colonna 'website' non trovata nella tabella")
            return 0
        
        # QUERY FINALE: Contiamo i siti con dominio .info
        # Nota: usiamo due condizioni perché un URL potrebbe essere
        # esempio.info oppure esempio.info/pagina
        query = f"""
            SELECT COUNT(*) FROM utenti
            WHERE "{website_col}" LIKE '%.info' OR "{website_col}" LIKE '%.info/%';
        """
        
        result = conn.execute(query).fetchone()
        count_info = result[0]
        logger.info(f"Numero di website con dominio di primo livello 'info': {count_info}")
        return count_info
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {e}")
        raise

def main():
    """Funzione principale che coordina tutti i passaggi.
    
    Ho strutturato lo script in funzioni separate per chiarezza.
    Ogni funzione ha una responsabilità ben precisa e questo
    rende il codice più facile da capire e mantenere.
    """
    # Iniziamo a misurare il tempo totale di esecuzione
    start_time = time.time()
    logger.info("Avvio script DuckDB...")
    
    # Verifichiamo che il file CSV esista
    if not os.path.exists(CSV_FILE):
        logger.error(f"File CSV non trovato: {CSV_FILE}")
        return
    
    conn = None
    try:
        # Fase 1: Analisi del CSV
        header = get_csv_header(CSV_FILE)
        logger.info(f"Il file CSV ha {len(header)} colonne")
        
        # Fase 2: Connessione al database
        conn = create_connection()
        
        # Fase 3: Caricamento dei dati
        rows_loaded = load_csv_to_database(conn, CSV_FILE)
        
        # Fase 4: Esecuzione della query richiesta
        info_domains_count = count_info_domain_websites(conn)
        
        # Fase 5: Visualizzazione dei risultati
        print("\nRISULTATI:")
        print(f"Totale righe caricate: {rows_loaded}")
        print(f"Website con dominio di primo livello 'info': {info_domains_count}")
        
    except Exception as e:
        # Gestione centralizzata degli errori
        logger.error(f"Errore nell'esecuzione dello script: {e}")
    finally:
        # Pulizia e chiusura della connessione
        if conn:
            conn.close()
            logger.info("Connessione al database chiusa.")
        
        # Calcolo e visualizzazione del tempo totale
        elapsed_time = time.time() - start_time
        logger.info(f"Script completato in {elapsed_time:.2f} secondi.")
        print(f"\nTempo totale di esecuzione: {elapsed_time:.2f} secondi.")

# Punto di ingresso dello script
if __name__ == "__main__":
    main() 