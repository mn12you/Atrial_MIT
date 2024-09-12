import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

source_folder = './contest_data_4s'  # Replace with your source folder path
destination_folder = './contest_data_4s_whaleteq'  # Replace with your destination folder path

def copy_folder_structure(src, dst):
    """
    Replicate the folder structure from `src` to `dst` without copying files.
    
    :param src: Source directory to replicate.
    :param dst: Destination directory where the structure will be replicated.
    """
    for dirpath, dirnames, filenames in os.walk(src):
        # Calculate the relative path from the source folder
        relative_path = os.path.relpath(dirpath, src)
        # Create the corresponding directory in the destination folder
        destination_path = os.path.join(dst, relative_path)
        # Create the directory in the destination if it does not exist
        os.makedirs(destination_path, exist_ok=True)

def process_file(src, dst, dirpath, file):
    """
    Process a single file, transform its content, and save it to the destination.
    """
    relative_path = os.path.relpath(dirpath, src)
    destination_path = os.path.join(dst, relative_path)
    data_dir = os.path.join(dirpath, file)
    save_path = os.path.join(destination_path, file[:-4]+".txt")
    
    # Read CSV file
    raw_data = pd.read_csv(data_dir)
    leadone = raw_data['data'].tolist()
    
    # Prepare the data frame for saving
    data_df = ["250", str(len(leadone)), "start", "Lead I"]
    data_df = data_df + leadone
    for lead_name in ["Lead II", "V1", "V2", "V3", "V4", "V5", "V6"]:
        data_df.append(lead_name)
        data_df = data_df + ([0] * len(leadone))
    
    # Convert to DataFrame and save as txt
    data_df = pd.DataFrame(data_df)
    data_df.to_csv(save_path, index=False, header=False)

def transform_to_whaleteq(src, dst):
    """
    Transform files from the source to the destination directory in parallel.
    """
    # Create a ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor() as executor:
        futures = []
        for dirpath, dirnames, filenames in os.walk(src):
            if filenames:  # If there are files in the directory
                # Ensure the destination directory exists
                relative_path = os.path.relpath(dirpath, src)
                destination_path = os.path.join(dst, relative_path)
                os.makedirs(destination_path, exist_ok=True)
                
                # Submit tasks for processing each file
                for file in filenames:
                    futures.append(executor.submit(process_file, src, dst, dirpath, file))
        
        # Optionally, wait for all tasks to complete (not necessary with 'with' block)
        for future in futures:
            future.result()  # This will raise exceptions if any occurred during processing

# Main execution

# Copy folder structure
copy_folder_structure(source_folder, destination_folder)

# Transform files in parallel
transform_to_whaleteq(source_folder, destination_folder)

print(f"Folder structure of '{source_folder}' has been replicated to '{destination_folder}'.")
