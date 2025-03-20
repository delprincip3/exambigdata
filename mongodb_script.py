#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymongo   # Libreria per MongoDB
import csv       # Per la lettura dei file CSV
import time      # Per misurare i tempi di esecuzione
import os        # Per operazioni sui file
import logging   # Per i log delle operazioni
from datetime import datetime  # Per gestire le date
from dotenv import load_dotenv  # Per caricare le variabili d'ambiente

# Carica le configurazioni dal file .env
# In questo modo possiamo cambiare i parametri senza modificare il codice
load_dotenv()

# Configurazione del logging - uso un formato con data e ora
# perché è utile per capire quando è avvenuto ciascun evento
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parametri di connessione MongoDB dalle variabili d'ambiente
# con valori di default se non sono presenti nel .env
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('MONGO_DB_NAME', 'esame_db')
COLLECTION_NAME = os.getenv('MONGO_COLLECTION', 'utenti')

# Percorso del file CSV da processare
CSV_FILE = os.getenv('CSV_FILE_8000', 'esercizi/input_8000.csv')

def connect_to_mongodb():
    """Connessione a MongoDB.
    
    Ho separato la connessione in una funzione dedicata per rendere
    il codice più chiaro e modulare. In questo modo è anche più facile
    gestire eventuali errori di connessione.
    """
    try:
        logger.info("Connessione a MongoDB...")
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        logger.info("Connessione a MongoDB stabilita con successo.")
        return client, db
    except Exception as e:
        logger.error(f"Errore durante la connessione a MongoDB: {e}")
        raise

def drop_collection(db):
    """Elimina la collection se esiste.
    
    Questo passaggio è necessario per poter rieseguire lo script più volte
    senza avere duplicazioni di dati. Nella vita reale, potrebbe essere 
    pericoloso, ma per un esercizio va bene così.
    """
    try:
        if COLLECTION_NAME in db.list_collection_names():
            logger.info(f"Eliminazione collection {COLLECTION_NAME} esistente...")
            db[COLLECTION_NAME].drop()
            logger.info("Collection eliminata.")
    except Exception as e:
        logger.error(f"Errore durante l'eliminazione della collection: {e}")
        raise

def load_csv_to_mongodb(db, csv_file):
    """Carica i dati dal CSV in MongoDB.
    
    PASSAGGIO CHIAVE: a differenza dei database SQL, MongoDB è schema-less,
    quindi possiamo caricare direttamente i dati senza definire prima la struttura.
    Ma dobbiamo comunque fare attenzione ai tipi di dato, specialmente le date.
    """
    try:
        collection = db[COLLECTION_NAME]
        logger.info(f"Caricamento dati dal file {csv_file}...")
        
        # Contatore per il report finale
        total_rows = 0
        documents = []
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)  # Legge la prima riga (intestazione)
            
            # Per ogni riga del CSV, creiamo un documento MongoDB
            for row in csv_reader:
                # APPROCCIO DINAMICO: creiamo il documento usando i nomi delle colonne dal header
                # Questo è meglio che usare indici fissi (row[0], row[1], ecc.) perché è più robusto
                # se la struttura del CSV cambia
                doc = {}
                for i, col_name in enumerate(header):
                    if i < len(row):  # Proteggiamo contro righe più corte del header
                        value = row[i]
                        # GESTIONE SPECIALE DELLE DATE: se la colonna sembra contenere una data
                        # proviamo a convertirla nel formato datetime di Python
                        if 'date' in col_name.lower() or 'data' in col_name.lower():
                            try:
                                value = datetime.strptime(value, "%Y-%m-%d") if value else None
                            except ValueError:
                                # Se il formato non è corretto, manteniamo il valore originale
                                pass
                        doc[col_name] = value
                
                documents.append(doc)
                total_rows += 1
                
                # OTTIMIZZAZIONE: Inseriamo in batch di 1000 documenti
                # Molto più efficiente che inserire un documento alla volta
                if len(documents) >= 1000:
                    collection.insert_many(documents)
                    logger.info(f"Caricati {total_rows} documenti...")
                    documents = []
        
        # Inseriamo i documenti rimanenti (meno di 1000)
        if documents:
            collection.insert_many(documents)
            
        logger.info(f"Caricamento completato: {total_rows} documenti inseriti.")
        return total_rows
    except Exception as e:
        logger.error(f"Errore durante il caricamento dei dati: {e}")
        raise

def count_2020_subscriptions(db):
    """Conta quante 'Subscription Date' sono dell'anno 2020.
    
    PARTE CRUCIALE: questa funzione implementa la query richiesta.
    L'ho resa robusta cercando il campo con la data di sottoscrizione
    anche se non si chiama esattamente "Subscription Date".
    """
    try:
        collection = db[COLLECTION_NAME]
        logger.info("Esecuzione query per contare iscrizioni del 2020...")
        
        # ROBUSTEZZA: Prima troviamo il campo che contiene le date di sottoscrizione
        # MongoDB è schema-less, quindi non possiamo fare come in SQL
        document_example = collection.find_one()
        date_field = None
        
        if document_example:
            # Esaminiamo tutti i campi del primo documento
            for field in document_example:
                field_lower = field.lower()
                # Cerchiamo campi che sembrano contenere date di sottoscrizione
                if ('subscription' in field_lower or 'iscrizione' in field_lower) and ('date' in field_lower or 'data' in field_lower):
                    date_field = field
                    break
        
        if not date_field:
            logger.error("Campo con data di sottoscrizione non trovato")
            return 0
            
        # Definiamo l'intervallo di date per il 2020
        # Nota: in MongoDB, le query di confronto sulle date funzionano
        # solo se sono oggetti datetime di Python
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 12, 31, 23, 59, 59)
        
        # QUERY MONGODB: Contiamo i documenti con data di sottoscrizione nel 2020
        result = collection.count_documents({
            date_field: {
                "$gte": start_date,  # maggiore o uguale alla data di inizio
                "$lte": end_date     # minore o uguale alla data di fine
            }
        })
        
        logger.info(f"Numero di iscrizioni del 2020: {result}")
        return result
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione della query: {e}")
        raise

def main():
    """Funzione principale che coordina tutte le operazioni.
    
    Ho organizzato il flusso dello script in passaggi logici ben definiti.
    Ciascun passaggio è gestito da una funzione dedicata, il che rende
    il codice più leggibile e più facile da mantenere.
    """
    # Misuriamo il tempo di esecuzione totale
    start_time = time.time()
    logger.info("Avvio script MongoDB...")
    
    # Verifichiamo che il file CSV esista
    if not os.path.exists(CSV_FILE):
        logger.error(f"File CSV non trovato: {CSV_FILE}")
        return
    
    client = None
    try:
        # Fase 1: Connessione a MongoDB
        client, db = connect_to_mongodb()
        
        # Fase 2: Pulizia della collection esistente
        drop_collection(db)
        
        # Fase 3: Caricamento dei dati dal CSV
        rows_loaded = load_csv_to_mongodb(db, CSV_FILE)
        
        # Fase 4: Esecuzione della query richiesta
        subscriptions_2020 = count_2020_subscriptions(db)
        
        # Fase 5: Visualizzazione dei risultati
        print("\nRISULTATI:")
        print(f"Totale documenti caricati: {rows_loaded}")
        print(f"Iscrizioni del 2020: {subscriptions_2020}")
        
    except Exception as e:
        # Gestione centralizzata degli errori
        logger.error(f"Errore nell'esecuzione dello script: {e}")
    finally:
        # Chiusura della connessione a MongoDB
        if client:
            client.close()
            logger.info("Connessione a MongoDB chiusa.")
        
        # Calcolo e visualizzazione del tempo totale
        elapsed_time = time.time() - start_time
        logger.info(f"Script completato in {elapsed_time:.2f} secondi.")
        print(f"\nTempo totale di esecuzione: {elapsed_time:.2f} secondi.")

# Punto di ingresso dello script
if __name__ == "__main__":
    main() 