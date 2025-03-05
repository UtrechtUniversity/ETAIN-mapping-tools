import pandas as pd
import helper_functions as hf
import numpy as np


def add_frequency_colums(measurement_df):
    #load earfcn to frequency mapping table
    earfcn_mapping_df = pd.read_csv('data\earfcn_frequency_ranges.csv', delimiter=';')

    earfcn_to_freq = {}
    for _, row in earfcn_mapping_df.iterrows():
        low_earfcn = row['earfcn_low']
        high_earfcn = row['earfcn_high']
        low_freq = row['mhz_low']
        high_freq = row['mhz_high']
        
        # Assuming linear mapping between EARFCN and frequency
        if low_earfcn != high_earfcn:
            slope = (high_freq - low_freq) / (high_earfcn - low_earfcn)
            intercept = low_freq - slope * low_earfcn
            for earfcn in range(int(low_earfcn), int(high_earfcn) + 1):
                freq = slope * earfcn + intercept
                earfcn_to_freq[earfcn] = freq
        else:
            earfcn_to_freq[int(low_earfcn)] = low_freq


    for i in range(10):  # Assuming you have LTE_0 to LTE_9
        earfcn_col = f'LTE_{i}_earfcn'
        freq_col = f'LTE_{i}_frequency'
        measurement_df[freq_col] = measurement_df[earfcn_col].apply(
            lambda earfcn: round(earfcn_to_freq.get(earfcn, None), 1) if earfcn_to_freq.get(earfcn, None) is not None else None
        )
    return measurement_df

def normalize_rssi(measurement_df):
    F0 = 1800  # Reference frequency in MHz

    # Define the normalization function
    def normalize_function(rssi, frequency):
        if frequency is None or pd.isna(frequency):  # Handle missing or None frequencies
            return None
        return round(rssi + 20 * np.log10(frequency / F0),2)

    # Apply the normalization function to each LTE_x_rssi column and create new LTE_x_rssi_norm columns
    for i in range(10):  # Assuming you have LTE_0 to LTE_9
        rssi_col = f'LTE_{i}_rssi'
        freq_col = f'LTE_{i}_frequency'
        rssi_norm_col = f'LTE_{i}_rssi_norm'  # New column for normalized values
        
        # Apply the normalization function and store results in the new column
        measurement_df[rssi_norm_col] = measurement_df.apply(
            lambda row: normalize_function(row[rssi_col], row[freq_col]), axis=1
        )
    return measurement_df

measurement_df = normalize_rssi(measurement_df)
measurement_df[['LTE_0_rssi', 'LTE_0_frequency', 'LTE_0_rssi_norm']]