from tfa_morlet56 import *
import sys
import os 
from pathlib import Path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from args import parse_args
from torch.utils.data import DataLoader
from tqdm import tqdm
import numpy as np
import pickle as pkl
from matplotlib import pyplot as plt
from data_mod.dataset import ECGDataset_all
from sklearn.model_selection import train_test_split
import random

def folder_generate(name):
    data_diff=["1","5","10","30","50","90","150"]

    for diff in data_diff:
        path=Path("./data",name+"_"+diff+"_cwt")
        if not path.exists():
            path.mkdir()
            train_path=Path(path,"train")
            train_path.mkdir()
            train_path_sub=Path(train_path,"data")
            train_path_sub.mkdir()
            train_path_sub=Path(train_path,"label")
            train_path_sub.mkdir()
            test_path=Path(path,"test")
            test_path.mkdir()
            test_path_sub=Path(test_path,"data")
            test_path_sub.mkdir()
            test_path_sub=Path(test_path,"label")
            test_path_sub.mkdir()
            val_path=Path(path,"val")
            val_path.mkdir()
            val_path_sub=Path(val_path,"data")
            val_path_sub.mkdir()
            val_path_sub=Path(val_path,"label")
            val_path_sub.mkdir()
        else:
            print("Dir exit.")
        path=Path("./result",name+"_"+diff)
        if not path.exists():
            os.makedirs(path)
        else:
            print("Dir exit.")

def cwt_data(dataloader,data_path,label_path):
    output_list=[]
    labels_list=[]
    for _, (data, labels) in enumerate(tqdm(dataloader)):
            output_list.append(data)
            labels_list.append(labels)
    y_data = np.vstack(output_list)
    y_label = np.vstack(labels_list)
    output_list=[]
    for i in range(y_data.shape[0]):
        data_np=y_data[i]
        data=np.squeeze(data_np,axis=0)
        output_list.append(np.expand_dims(tfa_morlet(data, 360, 4, 40, 0.643),axis=0))
    y_data = np.vstack(output_list)
    y_data=np.expand_dims(y_data,axis=1)

    
    print(diff)
    print(y_data.shape)
    print(y_label.shape)
    np.save(data_path,y_data)
    np.save(label_path,y_label)

if __name__=="__main__":
    arg=parse_args()
    datadir=arg.data_dir
    basepath="mitbih"
    random.seed(arg.seed)
    folder_generate(basepath)
    data_diff=["1","5","10","30","50","90","150"]
    for diff in data_diff:
        base_path=path=Path("./data",basepath+"_"+diff)
        base_path_save=path=Path("./data",basepath+"_"+diff+"_cwt")
        base_name=basepath+"_"+diff+"_cwt"

        train_data_path=Path(base_path,"train","data",basepath+"_"+diff+".npy")
        train_label_path=Path(base_path,"train","label",basepath+"_"+diff+".npy")
        val_data_path=Path(base_path,"val","data",basepath+"_"+diff+".npy")
        val_label_path=Path(base_path,"val","label",basepath+"_"+diff+".npy")
        test_data_path=Path(base_path,"test","data",basepath+"_"+diff+".npy")
        test_label_path=Path(base_path,"test","label",basepath+"_"+diff+".npy")

        train_data_path_save=Path(base_path_save,"train","data",base_name+".npy")
        train_label_path_save=Path(base_path_save,"train","label",base_name+".npy")
        val_data_path_save=Path(base_path_save,"val","data",base_name+".npy")
        val_label_path_save=Path(base_path_save,"val","label",base_name+".npy")
        test_data_path_save=Path(base_path_save,"test","data",base_name+".npy")
        test_label_path_save=Path(base_path_save,"test","label",base_name+".npy")
       
        train_dataset=ECGDataset_all(train_data_path,train_label_path)
        train_loader = DataLoader(train_dataset, batch_size=64, shuffle=False, num_workers=8)
        val_dataset=ECGDataset_all(val_data_path,val_label_path)
        val_loader = DataLoader(val_dataset, batch_size=64, shuffle=False, num_workers=8)
        test_dataset=ECGDataset_all(test_data_path,test_label_path)
        test_loader = DataLoader(test_dataset, batch_size=64, shuffle=False, num_workers=8)
        cwt_data(train_loader,train_data_path_save,train_label_path_save)
        cwt_data(val_loader,val_data_path_save,val_label_path_save)
        cwt_data(test_loader,test_data_path_save,test_label_path_save)
