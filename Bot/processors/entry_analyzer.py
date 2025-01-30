import datetime
from handlers import CustomLoggerHandler

class EntryAnalyzer:
    @staticmethod
    def check_entry(df_filtered, last_timestamp, logger):
        start_index = df_filtered[df_filtered['timestamp'] == last_timestamp].index[0] + 1
        loop_start_index = start_index - 4
        
        last_price_above_dema = None
        if loop_start_index >= 0:
            initial_row = df_filtered.iloc[loop_start_index]
            last_price_above_dema = initial_row['close'] > initial_row['DEMA']

        for current_index in range(loop_start_index, len(df_filtered)):
            current_row = df_filtered.iloc[current_index]
            prev_row = df_filtered.iloc[current_index - 1]
            
            current_above_dema = current_row['close'] > current_row['DEMA']

            if current_index < 4:
                if current_above_dema != last_price_above_dema:
                    if current_above_dema:
                        logger.log_entry_analysis(f"Price crossed above DEMA at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
                    else:
                        logger.log_entry_analysis(f"Price crossed below DEMA at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
                last_price_above_dema = current_above_dema
                continue

            prev_demas = df_filtered.iloc[current_index - 4:current_index]['DEMA'].values
            positive_slope = all(prev_demas[i] < prev_demas[i + 1] for i in range(len(prev_demas) - 1))
            negative_slope = all(prev_demas[i] > prev_demas[i + 1] for i in range(len(prev_demas) - 1))

            golden_cross = (
                (current_row['EMA_20'] > current_row['EMA_50'] >= current_row['EMA_200']) and
                (prev_row['EMA_20'] <= prev_row['EMA_50'] or prev_row['EMA_50'] <= prev_row['EMA_200'])
            )
            if golden_cross:
                logger.log_entry_analysis(f"Golden cross event at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
    

            death_cross = (
                (current_row['EMA_20'] < current_row['EMA_50'] < current_row['EMA_200']) and
                (prev_row['EMA_20'] >= prev_row['EMA_50'] or prev_row['EMA_50'] >= prev_row['EMA_200'])
            )
            if death_cross:
                logger.log_entry_analysis(f"Death cross event at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")

            if current_above_dema != last_price_above_dema:
                if current_above_dema:
                    logger.log_entry_analysis(f"Price crossed above DEMA at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
                else:
                    logger.log_entry_analysis(f"Price crossed below DEMA at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
            last_price_above_dema = current_above_dema

            if (current_row['high'] >= current_row['FBB_upper']):
                logger.log_entry_analysis(f"Price crossed upper FBB at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
    
            elif (current_row['low'] <= current_row['FBB_lower']):
                logger.log_entry_analysis(f"Price crossed lower FBB at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")

            if (
                golden_cross and
                current_above_dema and
                current_row['Direction'] == 1 and
                positive_slope
            ):
                logger.log_entry_analysis(f"Long entry signal at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")

            elif (
                death_cross and
                not current_above_dema and 
                current_row['Direction'] == -1 and
                negative_slope
            ):
                logger.log_entry_analysis(f"Short entry signal at timestamp: {current_row['timestamp'] + datetime.timedelta(hours=4)} UTC")
