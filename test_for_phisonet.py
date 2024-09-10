import physionet as pn

# Download a dataset
pn.download('ptbdb', 'data/ptbdb')

# List all datasets
pn.list_datasets()