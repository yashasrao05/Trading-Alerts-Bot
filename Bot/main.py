import datetime
import pandas as pd
from config import CONNECTION_STRING, BOT_TOKEN, CHAT_ID
from handlers import MongoDBHandler, DataFetcher, CustomLoggerHandler
from processors import DataProcessor, EntryAnalyzer, DataFormatter

def main():
    # Initialize logger
    logger = CustomLoggerHandler(bot_token=BOT_TOKEN, chat_id=CHAT_ID)

    try:
        # Initialize MongoDB connection
        mongo_handler = MongoDBHandler(CONNECTION_STRING, logger=logger)

        # Get last processed timestamp
        last_timestamp = mongo_handler.get_last_processed_timestamp()
        last_timestamp = datetime.datetime.strptime(last_timestamp, "%Y-%m-%d %H:%M:%S")
        
        # Fetch historical data from MongoDB
        try:
            last_450_df = mongo_handler.fetch_last_450_rows()
        except Exception as e:
            logger.log_error_with_code("E002", f"Database error while fetching last 450 rows: {str(e)}")
            raise

        # Process historical data
        try:
            indicators_df = pd.json_normalize(last_450_df['indicators'])
            db_df = pd.concat([last_450_df.drop('indicators', axis=1), indicators_df], axis=1)
            db_df['timestamp'] = pd.to_datetime(db_df['timestamp'])
            db_df['timestamp'] = db_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            db_df = db_df.sort_values('timestamp')
            db_df.drop(columns=['_id'], inplace=True)
        except Exception as e:
            logger.log_error_with_code("E006", f"Error while processing historical data: {str(e)}")
            raise

        # Fetch new data
        try:
            imported_df = DataFetcher.fetch_new_data(last_timestamp)
        except Exception as e:
            logger.log_error_with_code("E004", f"Error while fetching new data: {str(e)}")
            raise

        DataProcessor.clean_timestamps(imported_df)

        # Combine dataframes
        df = pd.concat([db_df, imported_df], ignore_index=True)
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Ensure we have enough historical data for calculations
        if len(df) < 450:
            logger.log_error_with_code("E001", "Insufficient historical data for accurate calculations")
            raise ValueError("Insufficient historical data for accurate calculations")

        # Calculate all indicators ensuring proper historical context
        try:
            df = DataProcessor.basic_indicators(df)
            df = DataProcessor.calculate_dema(df)  # Calculate DEMA before SuperTrend
            df = DataProcessor.add_supertrend(df)
            df = DataProcessor.add_FBB(df)
        except Exception as e:
            logger.log_error_with_code("E006", f"Error while calculating indicators: {str(e)}")
            raise

        # Drop any unnecessary columns
        df = df.drop(columns=['hl2', 'vwma_200', 'std_dev'], errors='ignore')
        df = df.dropna()

        # Filter data for analysis
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df_filtered = df[df['timestamp'] >= (last_timestamp - datetime.timedelta(hours=12))]
        df_filtered = df_filtered.reset_index(drop=True)

        # Perform entry analysis
        try:
            EntryAnalyzer.check_entry(df_filtered, last_timestamp, logger)
        except Exception as e:
            logger.log_error_with_code("E006", f"Error during entry analysis: {str(e)}")
            raise

        # Prepare data for MongoDB update
        start_index = df_filtered[df_filtered['timestamp'] == last_timestamp].index[0] + 1
        df = df_filtered.iloc[start_index:].reset_index(drop=True)

        # Convert to MongoDB format and update database
        try:
            mongo_data = DataFormatter.convert_to_mongo_format(df)
            mongo_handler.update_collection(mongo_data)
        except Exception as e:
            logger.log_error_with_code("E002", f"Error while updating database: {str(e)}")
            raise

    except ValueError as e:
        logger.log_error_with_code("E001", f"Data error: {str(e)}")
        raise e
    except Exception as e:
        logger.log_error_with_code("E005", f"Unexpected error: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
