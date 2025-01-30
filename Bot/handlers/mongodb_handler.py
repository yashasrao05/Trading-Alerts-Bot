import pandas as pd
from pymongo import MongoClient
from handlers.logging_handler import CustomLoggerHandler

class MongoDBHandler:
    def __init__(self, connection_string, logger=None):
        self.client = MongoClient(connection_string)
        self.db = self.client['OHCLV_indicators']
        self.btc_collection = self.db['BTC']

        # If no logger is provided, create a default one
        self.logger = logger if logger else CustomLoggerHandler()

    def get_last_processed_timestamp(self):
        last_document = self.btc_collection.find_one(sort=[('timestamp', -1)])
        return last_document['timestamp'] if last_document else None

    def fetch_last_450_rows(self):
        last_450_rows = self.btc_collection.find().sort('timestamp', -1).limit(450)
        df = pd.DataFrame(list(last_450_rows))
        df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
        return df

    def update_collection(self, mongo_data):
        new_documents = []
        for document in mongo_data:
            new_documents.append(document)
        
        if new_documents:
            try:
                self.btc_collection.insert_many(new_documents)
                # Log to file only, not to Telegram
                self.logger.log_info(f"Successfully inserted {len(new_documents)} new documents")
            except Exception as e:
                # Log error to file only
                error_msg = f"Error inserting documents: {e}"
                self.logger.log_error(error_msg)
        else:
            # Log to file only
            self.logger.log_info("No new documents to insert")