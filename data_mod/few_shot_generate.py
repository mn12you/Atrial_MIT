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
        else:
            print("Dir exit.")
        path=Path("./result",name+"_"+diff)
        if not path.exists():
            os.makedirs(path)
        else:
            print("Dir exit.")

def few_shot(dataloader,support_path,query_path,label_path,shot):
    query_set_list=[]
    query_set_label_list=[]
    support_set_list=[]
    for _, (data, labels) in enumerate(tqdm(dataloader)):
        query_set_list.append(data)
        query_set_label_list.append(labels)
        batch=data.shape[0]
        class_number=train_label.shape[-1]
        support_temp=[]
        for class_num in range(class_number):
            index_temp=[]
            for boots in range(shot):
                index_temp.append(random.sample(class_index[class_num], batch))
            support_temp.append(train_data[index_temp])
        support_set_list.append(np.stack(support_temp,axis=1).reshape(batch,class_number*shot,-1))
    query_set_data = np.vstack(query_set_list)
    query_set_label = np.vstack(query_set_label_list)
    support_set = np.vstack(support_set_list)
    print(query_set_data.shape)
    print(query_set_label.shape)
    print(support_set.shape)
    np.save(support_path,support_set)
    np.save(query_path,query_set_data)
    np.save(label_path,query_set_label)





    
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
    data_diff=["1","5","10","30","50","90","150"]
    for folds in range(10):
        
        for diff in data_diff:
            base_path=Path("./data",basepath+"_"+diff)
            test_data_path=Path(base_path,"test","data",basepath+"_"+diff+"_spe_fold"+str(folds)+".npy")
            test_label_path=Path(base_path,"test","label",basepath+"_"+diff+"_spe_fold"+str(folds)+".npy")
            val_data_path=Path(base_path,"val","data",basepath+"_"+diff+"_spe_fold"+str(folds)+".npy")
            val_label_path=Path(base_path,"val","label",basepath+"_"+diff+"_spe_fold"+str(folds)+".npy")
            train_data_path=Path(base_path,"train","data",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            train_label_path=Path(base_path,"train","label",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            base_path=Path("./data",basepath+"_"+diff+"_"+"pair")

            test_dataset=ECGDataset_all(test_data_path,test_label_path)
            test_loader = DataLoader(test_dataset, batch_size=1, shuffle=False, num_workers=1)
            val_dataset=ECGDataset_all(val_data_path,val_label_path)
            val_loader = DataLoader(val_dataset, batch_size=1, shuffle=False, num_workers=1)
            train_dataset=ECGDataset_all(train_data_path,train_label_path)
            train_loader = DataLoader(train_dataset, batch_size=64, shuffle=False, num_workers=8)
        
            output_list=[]
            labels_list=[]#1 the same; 0 different
            for _, (data, labels) in enumerate(tqdm(train_loader)):
                output_list.append(data)
                labels_list.append(labels)
            train_data = np.vstack(output_list)
            train_label = np.vstack(labels_list)
            class_index={}
            for class_num in range(train_label.shape[-1]):
                class_index[class_num]=np.where(train_label[:,class_num]==1)[0].tolist()

            shots=[1,5]

            for shot in shots:
                val_data_path_support=Path(base_path,"val","data",basepath+"_"+diff+"_pair_"+"support_"+str(shot)+"_"+"shot"+"_spe_fold"+str(folds)+".npy")
                val_data_path_query=Path(base_path,"val","data",basepath+"_"+diff+"_pair_"+"query_"+str(shot)+"_"+"shot"+"_spe_fold"+str(folds)+".npy")
                val_label_path_save=Path(base_path,"val","label",basepath+"_"+diff+"_pair_"+str(shot)+"_"+"shot"+"_spe_fold"+str(folds)+".npy")
                test_data_path_support=Path(base_path,"test","data",basepath+"_"+diff+"_pair_"+"support_"+str(shot)+"_"+"shot"+"_spe_fold"+str(folds)+".npy")
                test_data_path_query=Path(base_path,"test","data",basepath+"_"+diff+"_pair_"+"query_"+str(shot)+"_"+"shot"+"_spe_fold"+str(folds)+".npy")
                test_label_path_save=Path(base_path,"test","label",basepath+"_"+diff+"_pair_"+str(shot)+"_"+"shot"+"_spe_fold"+str(folds)+".npy")

                few_shot(val_loader,val_data_path_support,val_data_path_query,val_label_path_save,shot)
                few_shot(test_loader,test_data_path_support,test_data_path_query,test_label_path_save,shot)


            



            