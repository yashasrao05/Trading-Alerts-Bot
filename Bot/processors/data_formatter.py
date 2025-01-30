class DataFormatter:
    @staticmethod
    def convert_to_mongo_format(df):
        mongo_format = []
        for _, row in df.iterrows():
            mongo_entry = {
                "timestamp": str(row['timestamp']),
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close'],
                "volume": row['volume'],
                "indicators": {
                    "RSI": row['RSI'],
                    "ATR": row['ATR'],
                    "EMA_20": row['EMA_20'],
                    "EMA_50": row['EMA_50'],
                    "EMA_200": row['EMA_200'],
                    "DEMA": row['DEMA'],
                    "SuperTrend": row['SuperTrend'],
                    "Direction": row['Direction'],
                    "Signal": row['Signal'],
                    "SignalChange": row['SignalChange'],
                    "FBB_upper": row['FBB_upper'],
                    "FBB_lower": row['FBB_lower']
                }
            }
            mongo_format.append(mongo_entry)
        return mongo_format