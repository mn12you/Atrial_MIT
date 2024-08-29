import os
from pathlib import Path
import sys
data_base_dir="mit_bih_AF"
import wfdb
import pandas as pd 
import numpy as np
import scipy.signal as sp

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
        save_path="./"+data_base_dir+"/"+patient_id+"/"+patient_id+"_1.csv"
        np.savetxt(save_path, record_signal_lead_1, delimiter=",")
        save_path="./"+data_base_dir+"/"+patient_id+"/"+patient_id+"_2.csv"
        np.savetxt(save_path, record_signal_lead_2, delimiter=",")
        try:
            patient_annotation = wfdb.rdann("./mit/"+patient_id,extension="qrsc")
        except:
            patient_annotation = wfdb.rdann("./mit/"+patient_id,extension="qrs")
        patient_symbol=np.array(patient_annotation.symbol)
        patient_ann=np.stack((patient_annotation.sample,patient_symbol), axis=1)
        patient_df=pd.DataFrame(patient_ann, columns=['time', 'label'])
        save_path="./"+data_base_dir+"/"+patient_id+"/"+patient_id+"_ann.csv"
        patient_df.to_csv(save_path)
       

