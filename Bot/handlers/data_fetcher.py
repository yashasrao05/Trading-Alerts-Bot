import ccxt
import pandas as pd
import datetime
import time

class DataFetcher:
    @staticmethod
    def fetch_new_data(last_timestamp):
        symbol = 'BTC/USDT:USDT'
        timeframe = '4h'
        exchange = ccxt.bybit({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        last_timestamp = pd.to_datetime(last_timestamp).tz_localize('UTC') if last_timestamp.tzinfo is None else last_timestamp
        since = int(last_timestamp.timestamp() * 1000)
        
        now = datetime.datetime.now(datetime.timezone.utc)
        current_candle_start = now.replace(hour=now.hour - now.hour % 4, minute=0, second=0, microsecond=0)
        end_time = int(current_candle_start.timestamp() * 1000)
        
        all_ohlcv = []
        while since < end_time:
            try:
                ohlcv_batch = exchange.fetch_ohlcv(symbol, timeframe, since, limit=200)
                if len(ohlcv_batch) == 0:
                    break
                all_ohlcv.extend(ohlcv_batch)
                since = ohlcv_batch[-1][0] + 1
            
            except ccxt.NetworkError as e:
                print(f"Network error fetching data: {e}. Retrying in 30 seconds...")
                time.sleep(30)
            except ccxt.ExchangeError as e:
                print(f"Exchange error fetching data: {e}. Retrying in 30 seconds...")
                time.sleep(30)
            except Exception as e:
                print(f"Unexpected error fetching data: {e}. Skipping this batch.")
                since += 4 * 60 * 60 * 1000
                continue
        
        if not all_ohlcv:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        df = df[df['timestamp'] > last_timestamp]
        
        if not df.empty:
            last_candle_timestamp = df.iloc[-1]['timestamp']
            if last_candle_timestamp >= current_candle_start:
                df = df.iloc[:-1]
        
        return df