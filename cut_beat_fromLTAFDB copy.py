import os
from pathlib import Path
import wfdb
from scipy import signal
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import argparse

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", default='LTAFDB')
args = parser.parse_args()

# Parameters
window_size = 4
fs = 250  # Resampling frequency
pass_size = window_size * fs

# Directory setup
save_dir = "./contest_data_" + str(window_size) + "s_LTAFDB/"
data_base_dir = args.data_dir
SCRIPT_DIR = "./" + data_base_dir + "/"
data_record = Path(SCRIPT_DIR, 'RECORDS')

# Label to folder mapping
label_to_folder = {"AFIB": "Afib", "N": "Normal"}

# Bad records list (if any)
bad_rec = []

# Read records
with open(data_record) as f:
    records = f.readlines()

def process_patient_record(patient_id):
    """
    Process a single patient record.
    """
    patient_id = str(patient_id.strip())
    if patient_id in bad_rec:
        return  # Skip bad records

    # Read patient data
    try:
        patient_record = wfdb.rdrecord(SCRIPT_DIR + patient_id)
        record_signal = patient_record.p_signal[:, 0]

        patient_annotation = wfdb.rdann(SCRIPT_DIR + patient_id, extension="atr")
        clean_annotation = [ann.strip('(') for ann in patient_annotation.aux_note]
        patient_afib = np.stack((patient_annotation.sample, clean_annotation), axis=1)
        patient_afib_df = pd.DataFrame(patient_afib, columns=['time', 'label'])
        patient_afib_df = patient_afib_df[patient_afib_df['label'] != ""]
        patient_afib_df['time'] = patient_afib_df['time'].astype(int)

        patient_annotation = wfdb.rdann(SCRIPT_DIR + patient_id, extension="qrs")
        patient_qrs = np.stack((patient_annotation.sample, patient_annotation.symbol), axis=1)
        patient_qrs_df = pd.DataFrame(patient_qrs, columns=['time', 'label'])
        patient_qrs_df['time'] = patient_qrs_df['time'].astype(int)
    except Exception as e:
        print(f"Error reading record {patient_id}: {e}")
        return

    # Variables for processing
    qrs_index = 0
    afib_index = 0
    count = 0
    rhythm = label_to_folder.get(patient_afib_df.iloc[0]['label'], "Normal")
    rhythm_flag = False

    time_index_left = patient_afib_df.iloc[0]['time']
    time_index_right = time_index_left + pass_size

    while time_index_right <= len(record_signal):
        three_heartbeat = 0
        while (qrs_index < len(patient_qrs_df) and 
               time_index_left <= patient_qrs_df.iloc[qrs_index]['time'] <= time_index_right):
            qrs_index += 1
            three_heartbeat += 1
            if (afib_index < len(patient_afib_df) and 
                patient_qrs_df.iloc[qrs_index]['time'] >= patient_afib_df.iloc[afib_index]['time']):
                afib_index += 1
                if patient_afib_df.iloc[afib_index - 1]['label'] in label_to_folder:
                    rhythm = label_to_folder[patient_afib_df.iloc[afib_index - 1]['label']]
                else:
                    rhythm_flag = True
                three_heartbeat = 0
                break

        if three_heartbeat >= 3:
            file_path = os.path.join(save_dir, rhythm, patient_id)
            os.makedirs(file_path, exist_ok=True)
            save_path = os.path.join(file_path, f"{patient_id}_{count:05}.csv")
            
            # Resample and save the data
            data_df_temp = pd.DataFrame(signal.resample_poly(record_signal[time_index_left:time_index_right], 250, 128), columns=['data'])
            data_df_temp = data_df_temp.reset_index().rename(columns={'index': 'timestamp'})
            data_df_temp.to_csv(save_path, index=False)

            time_index_left = time_index_right + 1
            time_index_right = time_index_left + pass_size
            count += 1
        else:
            if qrs_index >= len(patient_qrs_df):
                break
            if rhythm_flag:
                rhythm_flag = False
                while (qrs_index < len(patient_qrs_df) and 
                       patient_qrs_df.iloc[qrs_index]['time'] <= patient_afib_df.iloc[afib_index]['time']):
                    qrs_index += 1
                time_index_left = patient_afib_df.iloc[afib_index]['time']
                time_index_right = time_index_left + pass_size
            else:
                time_index_left = patient_qrs_df.iloc[qrs_index]['time'] - int(0.1 * fs)
                if time_index_left < patient_afib_df.iloc[afib_index - 1]['time']:
                    time_index_left = patient_qrs_df.iloc[qrs_index]['time']
                time_index_right = time_index_left + pass_size

def main():
    # Parallel processing using ProcessPoolExecutor
    with ProcessPoolExecutor() as executor:
        executor.map(process_patient_record, records)

if __name__ == "__main__":
    main()
