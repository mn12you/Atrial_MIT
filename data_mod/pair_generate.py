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
from itertools import combinations

def folder_generate(name):
    data_diff=["1","5","10","30","50","90","150"]

    for diff in data_diff:
        path=Path("./data",name+"_"+diff+"_"+"pair")
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



    
if __name__=="__main__":
    arg=parse_args()
    datadir=arg.data_dir
    basepath="mitbih"
    random.seed(arg.seed)
    folder_generate(basepath)
    # output_list=[]
    # labels_list=[]
    # for _, (data, labels) in enumerate(tqdm(train_loader)):
    #     output_list.append(data)
    #     labels_list.append(labels)
    # y_data = np.vstack(output_list)
    # y_label = np.vstack(labels_list)
    # y_label=np.squeeze(y_label,axis=None)
    # print(y_label.shape)
    # print(y_data.shape)

    for folds in range(10):
            
        data_diff=["1","5","10","30","50","90","150"]
        for diff in data_diff:
            base_path=Path("./data",basepath+"_"+diff)
            train_data_path=Path(base_path,"train","data",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            train_label_path=Path(base_path,"train","label",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            base_path=Path("./data",basepath+"_"+diff+"_"+"pair")
            train_data_path_save_1=Path(base_path,"train","data",basepath+"_"+diff+"_"+"pair1"+"_fold"+str(folds)+".npy")
            train_data_path_save_2=Path(base_path,"train","data",basepath+"_"+diff+"_"+"pair2"+"_fold"+str(folds)+".npy")
            train_label_path_save=Path(base_path,"train","label",basepath+"_"+diff+"_"+"pair"+"_fold"+str(folds)+".npy")
            train_dataset=ECGDataset_all(train_data_path,train_label_path)
            train_loader = DataLoader(train_dataset, batch_size=64, shuffle=False, num_workers=8)

            output_list=[]
            labels_list=[]
            for _, (data, labels) in enumerate(tqdm(train_loader)):
                    output_list.append(data)
                    labels_list.append(labels)
            y_data = np.vstack(output_list)
            y_label = np.vstack(labels_list)

            output_list_1=[]
            output_list_2=[]
            final_label_list=[]#1 the same; 0 different
            for i, j in combinations(range(y_label.shape[0]), 2):
                output_list_1.append(i)
                output_list_2.append(j)
                if np.argmax(y_label[i])==np.argmax(y_label[j]):
                    final_label_list.append(np.array([[1]]))
                else:
                    final_label_list.append(np.array([[0]]))
            y_label = np.vstack(final_label_list)
            y_data1=y_data[output_list_1]
            y_data2=y_data[output_list_2]
            print(diff)
            print(y_data1.shape)
            print(y_data2.shape)
            print(y_label.shape)

            np.save(train_data_path_save_1,y_data1)
            np.save(train_data_path_save_2,y_data2)
            np.save(train_label_path_save,y_label)
            



            