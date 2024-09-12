import os
from pathlib import Path
import sys
import wfdb
import pandas as pd 
import numpy as np
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--data_dir",default='AFDB')
args = parser.parse_args()

window_size=6
fs=250
pass_size=window_size*fs

save_dir="./contest_data_"+str(window_size)+"s/"
data_base_dir=args.data_dir
SCRIPT_DIR="./"+data_base_dir+"/"
data_record=Path(SCRIPT_DIR,'RECORDS')


label_to_folder={"AFIB":"Afib","N":"Normal"}

bad_rec=['00735','03665', '04043', '04936', '05091', '06453', '08378', '08405', '08434', '08455']
records=[]
with open(data_record) as f:
    records = f.readlines()


for record in records:
    patient_id=str(record[:-1])
    print(patient_id)
    if patient_id  not in bad_rec:
        patient_record = wfdb.rdrecord(SCRIPT_DIR+patient_id)
        record_signal=patient_record.p_signal[:,0]
        patient_annotation = wfdb.rdann(SCRIPT_DIR + patient_id, extension="atr")
        clean_annotation = [ann.strip('(') for ann in patient_annotation.aux_note]
        patient_afib = np.stack((patient_annotation.sample, clean_annotation), axis=1)
        patient_afib_df = pd.DataFrame(patient_afib, columns=['time', 'label'])
        patient_afib_df ['time'] = patient_afib_df ['time'].astype(int)
        try:
            patient_annotation = wfdb.rdann(SCRIPT_DIR + patient_id, extension="qrsc")
        except:
            patient_annotation = wfdb.rdann(SCRIPT_DIR + patient_id, extension="qrs")
        patient_qrs = np.stack((patient_annotation.sample, patient_annotation.symbol), axis=1)
        patient_qrs_df = pd.DataFrame(patient_qrs, columns=['time', 'label'])
        patient_qrs_df ['time'] = patient_qrs_df ['time'].astype(int)

        qrs_index=0
        afib_index=0
        count=0
        rhythm=label_to_folder[patient_afib_df.iloc[0]['label']]
        rhythm_flag=False

        time_index_left=patient_afib_df.iloc[0]['time']
        time_index_right=time_index_left+pass_size
        while(time_index_right<=len(record_signal)):
            three_heartbeat=0
            while(qrs_index<len(patient_qrs_df) and patient_qrs_df.iloc[qrs_index]['time']>=time_index_left and patient_qrs_df.iloc[qrs_index]['time']<=time_index_right):
                qrs_index=qrs_index+1
                three_heartbeat=three_heartbeat+1
                if (afib_index<len(patient_afib_df) and patient_qrs_df.iloc[qrs_index]['time']>=patient_afib_df.iloc[afib_index]['time']):
                    afib_index=afib_index+1
                    if patient_afib_df.iloc[afib_index-1]['label'] in label_to_folder:
                        rhythm=label_to_folder[patient_afib_df.iloc[afib_index-1]['label']] 
                    else:
                        rhythm_flag=True
                    three_heartbeat=0
                    break
            if three_heartbeat>=3:
                file_path=save_dir+rhythm+'/'+patient_id+"/"
                if os.path.exists(file_path):
                    pass
                else:
                    os.makedirs(file_path, exist_ok=True)
                save_path=file_path+patient_id+"_"+f"{count:05}"+".csv"
                data_df_temp = pd.DataFrame(record_signal[time_index_left:time_index_right], columns=['data'])
                data_df_temp =data_df_temp.reset_index().rename(columns={'index': 'timestamp'})
                data_df_temp.to_csv(save_path,index=False)
                time_index_left=time_index_right+1
                time_index_right=time_index_left+pass_size
                count=count+1
            else:
                if qrs_index>=len(patient_qrs_df):
                    break
                if rhythm_flag:
                    rhythm_flag=False
                    while (qrs_index<len(patient_qrs_df) and patient_qrs_df.iloc[qrs_index]['time']<=patient_afib_df.iloc[afib_index]['time']):
                        qrs_index=qrs_index+1
                    time_index_left=patient_afib_df.iloc[afib_index]['time']
                    time_index_right=time_index_left+pass_size
                else:
                    time_index_left=patient_qrs_df.iloc[qrs_index]['time']-int(0.1*fs)
                    if (time_index_left < patient_afib_df.iloc[afib_index-1]['time'] ): 
                        time_index_left=patient_qrs_df.iloc[qrs_index]['time']
                    time_index_right=time_index_left+pass_size
            


            



