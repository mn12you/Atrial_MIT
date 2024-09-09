import os
from pathlib import Path
import sys
data_base_dir="mit_bih_AF"
import wfdb
import pandas as pd 
import numpy as np


SCRIPT_DIR="./mit"
data_record=Path(SCRIPT_DIR,'RECORDS')
df=pd.DataFrame(columns=['ecg_id','patient_id','labels','filename'])

bad_rec=['00735','03665', '04043', '04936', '05091', '06453', '08378', '08405', '08434', '08455']
records=[]
with open(data_record) as f:
    records = f.readlines()


for record in records:
    patient_id=str(record[:-1])
    if patient_id  not in bad_rec:
        patient_record = wfdb.rdrecord("./mit/"+patient_id)
        record_signal_lead_1=patient_record.p_signal[:,0]
        record_signal_lead_2=patient_record.p_signal[:,1]
        path=Path(data_base_dir,patient_id)
        file_path=Path(data_base_dir,patient_id)
        if file_path.exists():
            print("dir exists")
        else:
            file_path.mkdir()
        patient_data = np.stack((record_signal_lead_1, record_signal_lead_2), axis=1)
        patient_data_df = pd.DataFrame(patient_data, columns=['ECG1', 'ECG2'])
        save_path = "./" + data_base_dir + "/" + patient_id + "/" + patient_id + ".csv"
        patient_data_df.to_csv(save_path)

        patient_annotation = wfdb.rdann("./mit/" + patient_id, extension="atr")
        clean_annotation = [ann.strip('(') for ann in patient_annotation.aux_note]
        patient_ann = np.stack((patient_annotation.sample, clean_annotation), axis=1)
        patient_ann_df = pd.DataFrame(patient_ann, columns=['time', 'label'])
        save_path = "./" + data_base_dir + "/" + patient_id + "/" + patient_id + "_ann.csv"
        patient_ann_df.to_csv(save_path)

