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
from data_mod.dataset import ECGDataset
from sklearn.model_selection import train_test_split, KFold
import random


def folder_generate(name):
        path=Path("./data",name)
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
        path=Path("./result",name)
        if not path.exists():
            os.makedirs(path)
        else:
            print("Dir exit.")



    
if __name__=="__main__":
    arg=parse_args()
    datadir=arg.data_dir
    basepath="mitbih_all"
    random.seed(arg.seed)
    folder_generate(basepath)
    train_dataset = ECGDataset()
    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=False, num_workers=8, pin_memory=True,prefetch_factor=3)
    output_list=[]
    labels_list=[]
    for _, (data, labels) in enumerate(tqdm(train_loader)):
        output_list.append(data)
        labels_list.append(labels)
    y_data = np.vstack(output_list)
    y_label = np.vstack(labels_list)
    y_data=np.expand_dims(y_data, axis=1)
    y_label=np.squeeze(y_label,axis=None)
    print(y_label.shape)
    print(y_data.shape)
    # X_train, X_test, y_train, y_test = train_test_split(y_data, y_label, test_size=0.1, random_state=arg.seed)
    # X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.1, random_state=arg.seed)
    kf = KFold(n_splits=10,shuffle=True,random_state=arg.seed)

    for i, (train_index,test_index) in enumerate(kf.split(y_data)):
        X_train = y_data[train_index]
        y_train=y_label[train_index]
        X_test=y_data[test_index]
        y_test=y_label[test_index]
        X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.1, random_state=arg.seed)
        base_path=Path("./data",basepath)
        train_data_path=Path(base_path,"train","data",basepath+"_fold"+str(i)+".npy")
        train_label_path=Path(base_path,"train","label",basepath+"_fold"+str(i)+".npy")
        val_data_path=Path(base_path,"val","data",basepath+"_fold"+str(i)+".npy")
        val_label_path=Path(base_path,"val","label",basepath+"_fold"+str(i)+".npy")
        test_data_path=Path(base_path,"test","data",basepath+"_fold"+str(i)+".npy")
        test_label_path=Path(base_path,"test","label",basepath+"_fold"+str(i)+".npy")
        print(X_train.shape)
        print(y_train.shape)
        np.save(train_data_path,X_train)
        np.save(train_label_path,y_train)
        print(X_val.shape)
        print(y_val.shape)
        np.save(val_data_path,X_val)
        np.save(val_label_path,y_val)
        print(X_test.shape)
        print(y_test.shape)
        np.save(test_data_path,X_test)
        np.save(test_label_path,y_test)
            



            