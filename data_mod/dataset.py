import os
from torch.utils.data import Dataset
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelBinarizer
import pickle
import torch 

class ECGDataset(Dataset):
    def __init__(self, fs=360, data_dir="./mit_bih", label_csv="mit_bih_sub/mit_bih.csv"):
        super(ECGDataset, self).__init__()
        self.df=pd.read_csv(label_csv)
        with open(Path("labelbinarizer","lb.pkl"),'rb') as tokenizer:
            self.lb=pickle.load(tokenizer)

    def __getitem__(self,index:int):
        row=self.df.iloc[index]
        label=self.lb.transform([row.labels])
        data=np.load(row.filename)
        return data,label
    def __len__(self):
        return len(self.df)
    
class ECGDataset_all(Dataset):
    def __init__(self,  data_dir,label_dir):
        super(ECGDataset_all, self).__init__()
        self.data=np.load(data_dir)
        self.labels=np.load(label_dir)
        # self.data=pkl.load(open(data_dir,"rb"))
        # self.labels=pkl.load(open(data_dir,"rb"))

    def __getitem__(self, index: int):
        row = self.data[index]  
        labels=self.labels[index]
        return torch.from_numpy(row).float(),torch.from_numpy(labels).float()

    def __len__(self):
        return self.data.shape[0]
    
class ECGDataset_pair(Dataset):
    def __init__(self,  data_dir_1,data_dir_2,label_dir):
        super(ECGDataset_pair, self).__init__()
        self.data1=np.load(data_dir_1)
        self.data2=np.load(data_dir_2)
        self.labels=np.load(label_dir)
        # self.data=pkl.load(open(data_dir,"rb"))
        # self.labels=pkl.load(open(data_dir,"rb"))

    def __getitem__(self, index: int):
        data_1 = self.data1[index]
        data_2 = self.data2[index]    
        labels=self.labels[index]
        return torch.from_numpy(data_1).float(),torch.from_numpy(data_2).float(),torch.from_numpy(labels).float()

    def __len__(self):
        return self.data1.shape[0]
    
class ECGDataset_few_shot(Dataset):
    def __init__(self,  data_support_dir,data_query_dir,label_dir):
        super(ECGDataset_few_shot, self).__init__()
        self.data_support=np.load(data_support_dir)
        self.data_query=np.load(data_query_dir)
        self.labels=np.load(label_dir)
        # self.data=pkl.load(open(data_dir,"rb"))
        # self.labels=pkl.load(open(data_dir,"rb"))

    def __getitem__(self, index: int):
        data_1 = self.data_support[index]
        data_2 = self.data_query[index]    
        labels=self.labels[index]
        return torch.from_numpy(data_1).float(),torch.from_numpy(data_2).float(),torch.from_numpy(labels).float()

    def __len__(self):
        return self.data_support.shape[0]