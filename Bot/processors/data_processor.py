import pandas as pd
import numpy as np
import ta

class DataProcessor:
    @staticmethod
    def clean_timestamps(df):
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    @staticmethod
    def basic_indicators(df):
        calc_df = df.copy()
        calc_df = calc_df.sort_values('timestamp')
        
        calc_df['RSI'] = round(ta.momentum.rsi(calc_df['close'], window=14), 2)
        calc_df['ATR'] = round(ta.volatility.average_true_range(calc_df['high'], calc_df['low'], calc_df['close'], window=14), 2)
        calc_df['EMA_20'] = round(ta.trend.ema_indicator(calc_df['close'], window=20), 2)
        calc_df['EMA_50'] = round(ta.trend.ema_indicator(calc_df['close'], window=50), 2)
        calc_df['EMA_200'] = round(ta.trend.ema_indicator(calc_df['close'], window=200), 2)
        
        return calc_df

    @staticmethod
    def calculate_dema(df, length=200):
        calc_df = df.copy()
        calc_df = calc_df.sort_values('timestamp')
        
        ema1 = calc_df['close'].ewm(span=length, adjust=False, min_periods=length).mean()
        ema2 = ema1.ewm(span=length, adjust=False, min_periods=length).mean()
        calc_df['DEMA'] = (2 * ema1 - ema2).round(2)
        
        return calc_df

    @staticmethod
    def add_supertrend(df, atr_period=12, multiplier=3.0, change_atr=True):
        calc_df = df.copy()
        calc_df = calc_df.sort_values('timestamp')
        
        high, low, close = calc_df['high'], calc_df['low'], calc_df['close']
        
        tr1 = pd.DataFrame(high - low)
        tr2 = pd.DataFrame(abs(high - close.shift(1)))
        tr3 = pd.DataFrame(abs(low - close.shift(1)))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        if change_atr:
            atr = tr.ewm(alpha=1/atr_period, adjust=False, min_periods=atr_period).mean()
        else:
            atr = tr.rolling(window=atr_period, min_periods=atr_period).mean()
        
        hl2 = (high + low) / 2
        up = hl2 - (multiplier * atr)
        dn = hl2 + (multiplier * atr)
        
        supertrend = pd.Series(dn, name='SuperTrend')
        direction = pd.Series(np.ones(len(calc_df)), name='Direction')
        
        for i in range(1, len(calc_df)):
            if close.iloc[i] > supertrend.iloc[i-1]:
                supertrend.iloc[i] = max(up.iloc[i], supertrend.iloc[i-1])
            else:
                supertrend.iloc[i] = min(dn.iloc[i], supertrend.iloc[i-1])
            
            if close.iloc[i] < supertrend.iloc[i]:
                direction.iloc[i] = -1
            elif close.iloc[i] > supertrend.iloc[i]:
                direction.iloc[i] = 1
            else:
                direction.iloc[i] = direction.iloc[i-1]
                
            if direction.iloc[i] > 0 and direction.iloc[i-1] <= 0:
                supertrend.iloc[i] = min(dn.iloc[i], supertrend.iloc[i-1])
            elif direction.iloc[i] < 0 and direction.iloc[i-1] >= 0:
                supertrend.iloc[i] = max(up.iloc[i], supertrend.iloc[i-1])
        
        calc_df['SuperTrend'] = supertrend.round(2)
        calc_df['Direction'] = direction
        calc_df['Signal'] = np.where(direction == 1, 'Buy', np.where(direction == -1, 'Sell', None))
        calc_df['SignalChange'] = calc_df['Signal'] != calc_df['Signal'].shift(1)
        
        return calc_df

    @staticmethod
    def add_FBB(df, length=200, multiplier=3.0):
        df['hl2'] = (df['high'] + df['low']) / 2
        df['vwma_200'] = (df['hl2'] * df['volume']).rolling(window=length).sum() / df['volume'].rolling(window=length).sum()
        df['std_dev'] = df['hl2'].rolling(window=length).std()
        
        df['FBB_upper'] = round(df['vwma_200'] + (multiplier * df['std_dev']), 2)
        df['FBB_lower'] = round(df['vwma_200'] - (multiplier * df['std_dev']), 2)
        
        df = df.drop(columns=['hl2', 'vwma_200', 'std_dev'])
        return df
