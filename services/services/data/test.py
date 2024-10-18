import pandas as pd
from setup import Qdrant

data=pd.read_csv('processed_data.csv')

print(data.columns)
# payloads = data['processed_content']
    
# qdrant = Qdrant()

# collection_name = 'trend'
# vector_size = 384
# # qdrant.batch_insert_data(collection_name, payloads, processed_df, vector_size)
# qdrant.items_embedding_batch_insert_data(vector_size,collection_name, data, batch_size=500)
    
    # Assuming you have a DataFrame `df_loaded` and payloads are available
    # df_loaded = your pandas DataFrame here
    # payloads = df_loaded['processed_content']
    
 
