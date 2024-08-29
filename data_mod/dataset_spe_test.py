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

def data_gen(dataloader,data_path,label_path,rand_num):
    output_list=[]
    label_list=[]
    for _, (data, labels) in enumerate(tqdm(dataloader)):
        output_list.append(data)
        label_list.append(labels)
    data_array=np.vstack(output_list)
    label_array=np.vstack(label_list)
    class_number=5
    new_data=[]
    for class_num in range(class_number):
        class_index=np.where(label_array[:,class_num]==1)[0].tolist()
        if rand_num==100:
            if class_num==0:
                new_data=new_data+class_index
            elif class_num==1:
                number=rand_num+rand_num-len(new_data)
                print(number)
                new_data=new_data+random.sample(class_index,number)
            else:
                new_data=new_data+random.sample(class_index,rand_num)
        else:
            
             new_data=new_data+random.sample(class_index,rand_num)
    data=data_array[new_data]
    label=label_array[new_data]
    print(data.shape)
    print(label.shape)
    np.save(data_path,data)
    np.save(label_path,label)





    
if __name__=="__main__":
    arg=parse_args()
    datadir=arg.data_dir
    basepath="mitbih"
    random.seed(arg.seed)
    folder_generate(basepath)
    data_diff=["1","5","10","30","50","90","150"]
    
    for folds in range(10):

        for diff in data_diff:

            base_path="./data/"+basepath+"_"+diff

            test_data_path=Path(base_path,"test","data",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            test_label_path=Path(base_path,"test","label",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            val_data_path=Path(base_path,"val","data",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            val_label_path=Path(base_path,"val","label",basepath+"_"+diff+"_fold"+str(folds)+".npy")
            test_dataset=ECGDataset_all(test_data_path,test_label_path)
            test_loader = DataLoader(test_dataset, batch_size=32,shuffle=False, num_workers=4)
            val_dataset=ECGDataset_all(val_data_path,val_label_path)
            val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False, num_workers=4)

            val_data_path_save=Path(base_path,"val","data",basepath+"_"+diff+"_spe"+"_fold"+str(folds)+".npy")
            val_label_path_save=Path(base_path,"val","label",basepath+"_"+diff+"_spe"+"_fold"+str(folds)+".npy")

            test_data_path_save=Path(base_path,"test","data",basepath+"_"+diff+"_spe"+"_fold"+str(folds)+".npy")
            test_label_path_save=Path(base_path,"test","label",basepath+"_"+diff+"_spe"+"_fold"+str(folds)+".npy")

            data_gen(val_loader,val_data_path_save,val_label_path_save,rand_num=50)
            data_gen(test_loader,test_data_path_save,test_label_path_save,rand_num=100)


            



            