#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3  # Libreria per interagire con SQLite
import csv      # Per la lettura dei file CSV
import time     # Per misurare il tempo di esecuzione
import os       # Per operazioni sul sistema operativo
import logging  # Per i log di esecuzione
from dotenv import load_dotenv  # Per caricare variabili d'ambiente

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Configurazione logging - utile per tenere traccia delle operazioni
# Ho scelto un formato completo con timestamp per una migliore comprensione
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Percorso del file CSV e database SQLite
# Recupero i percorsi dal file .env, con valori predefiniti se non presenti
CSV_FILE = os.getenv('CSV_FILE_10000', 'esercizi/input_10000.csv')
DB_FILE = os.getenv('SQLITE_DB_FILE', 'utenti.db')

# Parole riservate in SQLite
# Ho inserito questa lista perché SQLite non permette di usare queste parole come nomi di colonne
# Sono tutte le parole chiave del linguaggio SQL che causerebbero errori di sintassi
RESERVED_WORDS = [
    'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
    'attach', 'autoincrement', 'before', 'begin', 'between', 'by', 'cascade', 'case',
    'cast', 'check', 'collate', 'column', 'commit', 'conflict', 'constraint', 'create',
    'cross', 'current_date', 'current_time', 'current_timestamp', 'database', 'default',
    'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct', 'drop', 'each',
    'else', 'end', 'escape', 'except', 'exclusive', 'exists', 'explain', 'fail', 'for',
    'foreign', 'from', 'full', 'glob', 'group', 'having', 'if', 'ignore', 'immediate',
    'in', 'index', 'indexed', 'initially', 'inner', 'insert', 'instead', 'intersect',
    'into', 'is', 'isnull', 'join', 'key', 'left', 'like', 'limit', 'match', 'natural',
    'no', 'not', 'notnull', 'null', 'of', 'offset', 'on', 'or', 'order', 'outer', 'plan',
    'pragma', 'primary', 'query', 'raise', 'recursive', 'references', 'regexp', 'reindex',
    'release', 'rename', 'replace', 'restrict', 'right', 'rollback', 'row', 'savepoint',
    'select', 'set', 'table', 'temp', 'temporary', 'then', 'to', 'transaction', 'trigger',
    'union', 'unique', 'update', 'using', 'vacuum', 'values', 'view', 'virtual', 'when',
    'where', 'with', 'without'
]

def create_connection():
    """Crea una connessione al database SQLite.
    
    Ho scelto di usare una funzione separata per questo perché rende il codice più
    organizzato e permette di gestire eventuali errori di connessione.
    """
    try:
        logger.info(f"Connessione al database SQLite {DB_FILE}...")
        conn = sqlite3.connect(DB_FILE)
        logger.info("Connessione stabilita con successo.")
        return conn
    except Exception as e:
        logger.error(f"Errore durante la connessione al database: {e}")
        raise

def get_csv_column_count(csv_file):
    """Ottiene il numero di colonne dal file CSV.
    
    Questa funzione è importante perché il nostro script deve adattarsi
    a CSV con diverso numero di colonne.
    """
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)  # Leggiamo solo la prima riga (intestazione)
            return len(header)
    except Exception as e:
        logger.error(f"Errore durante la lettura dell'intestazione CSV: {e}")
        raise

def create_table(conn, column_count):
    """Crea la tabella nel database con il numero corretto di colonne.
    
    Questa è una parte cruciale: invece di creare una tabella fissa,
    analizziamo l'intestazione del CSV e creiamo colonne con gli stessi nomi.
    Mi è capitato in passato di avere problemi con i CSV che cambiano struttura,
    quindi questo approccio rende lo script più robusto.
    """
    try:
        cursor = conn.cursor()
        logger.info("Creazione della tabella utenti...")
        
        # Drop table if exists - se la tabella esiste già, la eliminiamo
        cursor.execute("DROP TABLE IF EXISTS utenti;")
        
        # Creiamo la tabella con il numero corretto di colonne
        # Prima otteniamo l'intestazione dal CSV per usare i nomi corretti
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)
            
            # PASSAGGIO IMPORTANTE: Creiamo la definizione della tabella
            # con una colonna ID aggiuntiva come chiave primaria
            column_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
            for col_name in header:
                # Sanitizzo il nome della colonna: tolgo spazi, metto tutto in minuscolo
                safe_col_name = col_name.lower().replace(' ', '_')
                
                # PUNTO CRITICO: Se il nome è una parola riservata, aggiungiamo un suffisso
                # Questo evita errori di sintassi SQL (es. "index" è una parola riservata)
                if safe_col_name.lower() in [w.lower() for w in RESERVED_WORDS]:
                    safe_col_name = f"{safe_col_name}_col"
                
                # Mettiamo il nome della colonna tra virgolette per sicurezza
                column_defs.append(f'"{safe_col_name}" TEXT')
            
            # Costruiamo e eseguiamo la query SQL per creare la tabella
            create_sql = f"CREATE TABLE utenti ({', '.join(column_defs)});"
            logger.info(f"Query creazione tabella: {create_sql}")
            cursor.execute(create_sql)
        
        conn.commit()  # Confermiamo i cambiamenti nel database
        logger.info(f"Tabella creata con successo con {column_count} colonne.")
    except Exception as e:
        logger.error(f"Errore durante la creazione della tabella: {e}")
        raise

def load_csv_to_database(conn, csv_file):
    """Carica i dati dal CSV nel database SQLite.
    
    Questa è la parte più pesante in termini di calcolo.
    Ho implementato transazioni per rendere l'inserimento più veloce,
    altrimenti con file grandi potrebbe richiedere molto tempo.
    """
    try:
        cursor = conn.cursor()
        logger.info(f"Caricamento dati dal file {csv_file}...")
        
        # Conta le righe per il report finale
        total_rows = 0
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)  # Salta l'intestazione
            
            # PASSAGGIO CHIAVE: Generiamo nomi di colonne sicuri
            # con lo stesso procedimento usato nella creazione della tabella
            safe_column_names = []
            for col_name in header:
                # Sanitizzo il nome della colonna e gestisco le parole riservate
                safe_col_name = col_name.lower().replace(' ', '_')
                
                # Se il nome è una parola riservata, aggiungiamo il suffisso
                if safe_col_name.lower() in [w.lower() for w in RESERVED_WORDS]:
                    safe_col_name = f"{safe_col_name}_col"
                
                safe_column_names.append(safe_col_name)
            
            # TECNICA IMPORTANTE: Generiamo la query INSERT dinamicamente 
            # così funziona indipendentemente dal numero di colonne
            placeholders = ', '.join(['?' for _ in header])  # Crea la serie di ? per i parametri
            column_names = ', '.join([f'"{name}"' for name in safe_column_names])
            insert_sql = f"INSERT INTO utenti ({column_names}) VALUES ({placeholders})"
            
            # OTTIMIZZAZIONE: Utilizziamo le transazioni per migliorare le prestazioni
            # Senza transazioni, ogni INSERT farebbe una scrittura su disco, molto lento
            conn.execute("BEGIN TRANSACTION")
            
            # Inseriamo ogni riga del CSV nella tabella
            for row in csv_reader:
                cursor.execute(insert_sql, row)
                total_rows += 1
                
                # Ogni 1000 righe, facciamo commit e iniziamo una nuova transazione
                # Questo bilancia velocità e uso di memoria
                if total_rows % 1000 == 0:
                    logger.info(f"Caricate {total_rows} righe...")
                    conn.commit()
                    conn.execute("BEGIN TRANSACTION")
        
        # Commit finale per le righe rimanenti
        conn.commit()
        logger.info(f"Caricamento completato: {total_rows} righe inserite.")
        return total_rows
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati: {e}")
        conn.rollback()  # Annulliamo le modifiche in caso di errore
        raise

def count_https_websites(conn):
    """Conta quanti link in Website iniziano con 'https://'
    
    PARTE CRUCIALE: questa è la funzione che implementa la query richiesta
    nell'esercizio. Ho preferito un approccio flessibile che cerca la
    colonna giusta anche se il nome non è esattamente 'website'.
    """
    try:
        cursor = conn.cursor()
        logger.info("Esecuzione query per contare website che iniziano con 'https://'...")
        
        # PASSAGGIO IMPORTANTE: Verifichiamo prima quale colonna contiene i siti web
        # Questo è utile perché il CSV potrebbe avere nomi di colonne diversi
        cursor.execute("PRAGMA table_info(utenti)")
        columns = cursor.fetchall()
        website_col = None
        
        # Cerchiamo una colonna che potrebbe contenere informazioni sul sito web
        for col in columns:
            col_name = col[1].lower()  # Il nome della colonna è nella posizione 1
            # Cerchiamo nomi come 'website', 'web_site', 'site_web', ecc.
            if col_name == 'website' or 'website' in col_name or 'site' in col_name or 'sito' in col_name:
                website_col = col[1]
                break
        
        # Se non troviamo la colonna, impossibile procedere
        if not website_col:
            logger.error("Colonna 'website' non trovata nella tabella")
            return 0
        
        # QUERY FINALE: Contiamo quanti siti iniziano con https://
        cursor.execute(f"""
            SELECT COUNT(*) FROM utenti
            WHERE "{website_col}" LIKE 'https://%';
        """)
        
        result = cursor.fetchone()[0]  # Prendiamo il primo valore del risultato
        logger.info(f"Numero di website che iniziano con 'https://': {result}")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {e}")
        raise

def main():
    """Funzione principale: coordina l'esecuzione delle varie fasi
    
    Ho strutturato il programma in modo che tutte le operazioni siano
    ben separate in funzioni, così è più facile capire il flusso di esecuzione.
    """
    # Misuriamo il tempo di esecuzione totale
    start_time = time.time()
    logger.info("Avvio script SQLite...")
    
    # Verifica esistenza del file CSV
    if not os.path.exists(CSV_FILE):
        logger.error(f"File CSV non trovato: {CSV_FILE}")
        return
    
    conn = None
    try:
        # Fase 1: Otteniamo il numero di colonne dal CSV
        column_count = get_csv_column_count(CSV_FILE)
        logger.info(f"Il file CSV ha {column_count} colonne")
        
        # Fase 2: Connessione al database
        conn = create_connection()
        
        # Fase 3: Creazione dello schema della tabella
        create_table(conn, column_count)
        
        # Fase 4: Caricamento dei dati dal CSV
        rows_loaded = load_csv_to_database(conn, CSV_FILE)
        
        # Fase 5: Esecuzione della query richiesta
        https_websites_count = count_https_websites(conn)
        
        # Fase 6: Visualizzazione dei risultati
        print("\nRISULTATI:")
        print(f"Totale righe caricate: {rows_loaded}")
        print(f"Website che iniziano con 'https://': {https_websites_count}")
        
    except Exception as e:
        # Gestione centralizzata degli errori
        logger.error(f"Errore nell'esecuzione dello script: {e}")
    finally:
        # Chiusura delle risorse, anche in caso di errore
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