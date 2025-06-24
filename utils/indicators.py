import pandas as pd
import numpy as np

def calculate_donchian(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """
    Calculate the Donchian Channel for a given DataFrame.
    The Donchian Channel is defined by the highest high and lowest low over a specified period.
    This function adds two new columns to the DataFrame: 'donchian_high' and 'donchian_low'.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'high' and 'low' columns.
        period (int): The number of periods to consider for the Donchian Channel.
    Returns:
        pd.DataFrame: DataFrame with additional columns 'donchian_high' and 'donchian_low'.
    """
    df['donchian_high'] = df['high'].rolling(window=3).max()
    df['donchian_low'] = df['low'].rolling(window=3).min()

    return df



def calculate_adx(df: pd.DataFrame, period: int = 14, return_all: bool = False) -> pd.DataFrame:
    """
    Calculate the Average Directional Index (ADX) for a given DataFrame using optimized NumPy operations.
    The ADX is a measure of trend strength and is derived from the smoothed directional movement
    indicators (DI+ and DI-). It is calculated using the True Range (TR) and the directional movements
    (DM+ and DM-).

    Parameters:
        df (pd.DataFrame): DataFrame containing 'high', 'low', and 'close' columns.
        period (int): The number of periods to consider for the ADX calculation.
        return_all (bool): If True, returns all intermediate columns used in the calculation.

    Returns:
        pd.DataFrame: DataFrame with additional columns 'plus_di', 'minus_di', and 'adx'.
    
    Note: The first 2*period-2 rows will contain NaNs due to initialization.
    """
    if not {'high', 'low', 'close'}.issubset(df.columns):
        raise ValueError("DataFrame must contain 'high', 'low', and 'close' columns.")

    df = df.copy()
    df_og = df.copy()

    # Calculate True Range (TR)
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values

    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan

    tr = np.maximum.reduce([
        high - low,
        np.abs(high - prev_close),
        np.abs(low - prev_close)
    ])
    df['tr'] = tr

    # Calculate directional movements
    high_diff = high - np.roll(high, 1)
    low_diff = np.roll(low, 1) - low

    plus_dm = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
    minus_dm = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)

    df['plus_dm'] = plus_dm
    df['minus_dm'] = minus_dm

    # Wilder's smoothing using exponential weighted moving average
    df['tr_smooth'] = pd.Series(tr).ewm(alpha=1/period, adjust=False).mean()
    df['plus_dm_smooth'] = pd.Series(plus_dm).ewm(alpha=1/period, adjust=False).mean()
    df['minus_dm_smooth'] = pd.Series(minus_dm).ewm(alpha=1/period, adjust=False).mean()

    # Calculate DI values
    epsilon = 1e-10  # small value to avoid division by zero
    df['plus_di'] = 100 * df['plus_dm_smooth'] / (df['tr_smooth'] + epsilon)
    df['minus_di'] = 100 * df['minus_dm_smooth'] / (df['tr_smooth'] + epsilon)

    # Calculate DX
    df['dx'] = 100 * np.abs(df['plus_di'] - df['minus_di']) / (df['plus_di'] + df['minus_di'] + epsilon)

    # Calculate ADX
    df['adx'] = df['dx'].ewm(alpha=1/period, adjust=False).mean()

    if return_all:
        return df

    result = df[['plus_di', 'minus_di', 'adx']]
    return pd.concat([df_og, result], axis=1)


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate the Average True Range (ATR) using Wilder's smoothing.

    Parameters:
        df (pd.DataFrame): DataFrame with 'high', 'low', and 'close' columns.
        period (int): Number of periods for the ATR.

    Returns:
        pd.Series: ATR values.
    """
    if not {'high', 'low', 'close'}.issubset(df.columns):
        raise ValueError("DataFrame must contain 'high', 'low', and 'close' columns.")
    
    high = df['high']
    low = df['low']
    close = df['close']

    tr = np.maximum.reduce([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ])

    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr
