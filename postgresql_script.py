#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2  # Libreria per connettersi a PostgreSQL
import csv       # Per la gestione dei file CSV
import time      # Per misurare il tempo di esecuzione
import os        # Per operazioni sui file e percorsi
import logging   # Per i log delle operazioni
from dotenv import load_dotenv  # Per caricare le configurazioni dal file .env

# Carica le variabili d'ambiente dal file .env
# Questo permette di modificare le configurazioni senza toccare il codice
load_dotenv()

# Configurazione del sistema di logging
# Ho scelto questo formato perché include data e ora, utile per debug
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parametri di connessione al database da variabili d'ambiente
# Se non sono definite nel .env, usa i valori predefiniti
DB_PARAMS = {
    'dbname': os.getenv('PG_DBNAME', 'postgres'),
    'user': os.getenv('PG_USER', 'postgres'),
    'password': os.getenv('PG_PASSWORD', 'postgres'),
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': os.getenv('PG_PORT', '5432')
}

# Percorso del file CSV da processare
CSV_FILE = os.getenv('CSV_FILE_5000', 'esercizi/input_5000.csv')

def get_csv_column_count(csv_file):
    """Ottiene il numero di colonne e l'intestazione dal file CSV.
    
    È importante analizzare il CSV prima di creare la tabella, perché 
    non sappiamo a priori quante colonne contiene o come si chiamano.
    """
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)  # Legge la prima riga (intestazione)
            return len(header), header
    except Exception as e:
        logger.error(f"Errore durante la lettura dell'intestazione CSV: {e}")
        raise

def create_table(conn, header):
    """Crea la tabella nel database in base alle colonne del CSV.
    
    NOTA IMPORTANTE: Invece di hardcodare i nomi delle colonne, creiamo 
    la tabella dinamicamente in base all'intestazione del CSV.
    Ho imparato a mie spese che i file CSV possono cambiare struttura...
    """
    try:
        cursor = conn.cursor()
        logger.info("Creazione della tabella utenti...")
        
        # Eliminiamo la tabella se esiste già (così possiamo rieseguire lo script)
        cursor.execute("DROP TABLE IF EXISTS utenti;")
        
        # PASSAGGIO CRITICO: Creiamo la tabella dinamicamente basandoci sull'intestazione del CSV
        # Aggiungiamo anche un ID autoincrementante come chiave primaria
        column_defs = ["id SERIAL PRIMARY KEY"]
        for col_name in header:
            # Sanitizziamo il nome della colonna (spazi e maiuscole possono causare problemi)
            safe_col_name = col_name.lower().replace(' ', '_')
            column_defs.append(f"{safe_col_name} VARCHAR(255)")
        
        # Costruiamo e eseguiamo la query SQL per creare la tabella
        create_sql = f"CREATE TABLE utenti ({', '.join(column_defs)});"
        cursor.execute(create_sql)
        
        conn.commit()  # Confermo le modifiche
        logger.info(f"Tabella creata con successo con {len(header)} colonne.")
    except Exception as e:
        logger.error(f"Errore durante la creazione della tabella: {e}")
        raise

def load_csv_to_database(conn, csv_file, header):
    """Carica i dati dal CSV nel database PostgreSQL.
    
    Questa è la funzione più "pesante" in termini di tempo di esecuzione,
    specialmente con CSV di grandi dimensioni.
    """
    try:
        cursor = conn.cursor()
        logger.info(f"Caricamento dati dal file {csv_file}...")
        
        # Contatore per tracciare l'avanzamento
        total_rows = 0
        
        # OTTIMIZZAZIONE: Generiamo la query INSERT dinamicamente
        # basata sul numero di colonne nel CSV
        column_names = ', '.join([col.lower().replace(' ', '_') for col in header])
        placeholders = ', '.join(['%s' for _ in header])  # PostgreSQL usa %s per i parametri
        insert_sql = f"INSERT INTO utenti ({column_names}) VALUES ({placeholders})"
        
        # Apriamo il file e iniziamo a processarlo
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Saltiamo la riga di intestazione
            
            # Processiamo ogni riga del CSV e la inseriamo nel database
            for row in csv_reader:
                cursor.execute(insert_sql, row)
                total_rows += 1
                
                # Facciamo commit ogni 1000 righe per bilanciare
                # performance e uso della memoria
                if total_rows % 1000 == 0:
                    logger.info(f"Caricate {total_rows} righe...")
                    conn.commit()
        
        # Commit finale per le righe rimanenti
        conn.commit()
        logger.info(f"Caricamento completato: {total_rows} righe inserite.")
        return total_rows
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati: {e}")
        raise

def count_com_emails(conn):
    """Conta quante email terminano con '.com'
    
    PARTE CRUCIALE: questa funzione implementa la query richiesta nell'esercizio.
    L'ho resa robusta per gestire diversi possibili nomi di colonna per l'email.
    """
    try:
        cursor = conn.cursor()
        logger.info("Esecuzione query per contare email che terminano con '.com'...")
        
        # ROBUSTEZZA: Prima troviamo quale colonna contiene le email
        # Non diamo per scontato che si chiami esattamente 'email'
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'utenti' 
            AND table_schema = 'public'
        """)
        columns = cursor.fetchall()
        email_col = None
        
        # Cerchiamo una colonna che probabilmente contiene email
        for col in columns:
            col_name = col[0].lower()
            if col_name == 'email' or 'email' in col_name or 'mail' in col_name:
                email_col = col[0]
                break
        
        # Se non troviamo la colonna, impossibile procedere
        if not email_col:
            logger.error("Colonna 'email' non trovata nella tabella")
            return 0
        
        # QUERY FINALE: Contiamo le email che terminano con .com
        # Usando una query parametrizzata per sicurezza
        cursor.execute(f"""
            SELECT COUNT(*) FROM utenti
            WHERE {email_col} LIKE '%.com';
        """)
        
        # Estraggo il risultato (è un numero singolo)
        result = cursor.fetchone()[0]
        logger.info(f"Numero di email che terminano con '.com': {result}")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {e}")
        raise

def main():
    """Funzione principale che coordina l'esecuzione dello script.
    
    Ho diviso il codice in funzioni separate per chiarezza e manutenibilità.
    Ogni funzione ha una responsabilità ben definita.
    """
    # Iniziamo a misurare il tempo di esecuzione
    start_time = time.time()
    logger.info("Avvio script PostgreSQL...")
    
    # Verifico che il file CSV esista prima di procedere
    if not os.path.exists(CSV_FILE):
        logger.error(f"File CSV non trovato: {CSV_FILE}")
        return
    
    conn = None
    try:
        # Fase 1: Analisi del CSV
        _, header = get_csv_column_count(CSV_FILE)
        logger.info(f"Il file CSV ha {len(header)} colonne")
        
        # Fase 2: Connessione al database
        logger.info("Connessione al database PostgreSQL...")
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Fase 3: Creazione della tabella
        create_table(conn, header)
        
        # Fase 4: Caricamento dei dati
        rows_loaded = load_csv_to_database(conn, CSV_FILE, header)
        
        # Fase 5: Esecuzione della query richiesta
        com_emails_count = count_com_emails(conn)
        
        # Fase 6: Visualizzazione dei risultati
        print("\nRISULTATI:")
        print(f"Totale righe caricate: {rows_loaded}")
        print(f"Email che terminano con '.com': {com_emails_count}")
        
    except Exception as e:
        # Gestione centralizzata degli errori
        logger.error(f"Errore nell'esecuzione dello script: {e}")
    finally:
        # Pulizia: Chiusura della connessione
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