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


if __name__=="__main__":
    arg=parse_args()
    datadir=arg.data_dir
    basepath="mitbih"
    random.seed(arg.seed)
    train_dataset = ECGDataset()
    train_loader = DataLoader(train_dataset, batch_size=64, shuffle=False, num_workers=8, pin_memory=True,prefetch_factor=3)
    output_list=[]
    labels_list=[]
    for _, (data, labels) in enumerate(tqdm(train_loader)):
        output_list.append(data)
        labels_list.append(labels)
    y_data = np.vstack(output_list)
    y_label = np.vstack(labels_list)
    for data,label in zip(y_data,y_label):
        if np.argmax(label)==2:
            print(":")