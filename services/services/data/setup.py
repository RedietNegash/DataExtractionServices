import ast
import json
from qdrant_client import QdrantClient
from qdrant_client.http import models
import os
import traceback
import pandas as pd


class Qdrant:

    def __init__(self):
        try:
            self.client = QdrantClient(host='localhost', port=6333)
            print("Qdrant connected")
        except Exception as e:
            print(f'Qdrant connection failed: {e}')

    def get_create_collection(self, collection_name, vector_size): 
        try:
            self.client.get_collection(collection_name)
        except Exception:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.DOT)
            )
            print(f"Collection '{collection_name}' CREATED.")
    def batch_insert_data(self, collection_name, payloads, data, vector_size):
        try:
            data['id'] = list(map(int, data.index))  
            
            payload = [{"label": payloads[i]} for i in range(len(data))]
            
            self.get_create_collection(collection_name, vector_size)
            
            self.client.upsert(
                collection_name=collection_name,
                points=models.Batch(
                    ids=data['id'],
                    vectors=data['embedding'].tolist(),
                    payloads=payload
                )
            )
            print(f"Vectors saved to the {collection_name} collection")
        except:
            traceback.print_exc()
    def items_embedding_batch_insert_data(self, vector_size,collection_name: str, data: pd.DataFrame, batch_size=500) -> None:
        """
        Inserts vectors and associated data into the database in batches.

        Args:
            filtered_df (pd.DataFrame): The dataframe containing content information and embeddings.
            batch_size (int): The number of items to process in each batch.
        """
        
        for batch in range(0, len(data['embedding']), batch_size):
            batch_df = data[batch:batch + batch_size]
            print('------------batch content-----------', batch_df.columns)
            batch_df["id"] = batch_df["id"].astype(int)
            import ast
            try:
                batch_df["embed"] = batch_df["embedding"].apply(ast.literal_eval)
            except:
                batch_df["embed"] = batch_df["embedding"]
                
            payloads_list = [
                {
                    "id": item.id,
                    "processed_content":item.processed_content,
                    
                }
                for item in batch_df.itertuples() ]
            
            try:
                self.get_create_collection(collection_name, vector_size,type=None)
            except:
                print("failed to create collection name")

            try:
                self.client.upsert(
                    collection_name=collection_name,
                    points=models.Batch(
                        ids=batch_df["id"].tolist(),
                        vectors=batch_df["embed"].tolist(),
                        payloads=payloads_list,
                    ),)
                print("embedding saved")
            except:
                traceback.print_exc()

    def query_and_search(self, user_id, search_payload, query_payload, collection):
        try:
            items_label = models.Filter(must=[models.FieldCondition(key="label", match=models.MatchValue(value=query_payload))])
            user_vector = self.client.retrieve(collection_name=collection, ids=[user_id], with_vectors=True)

            if user_vector[0].payload.get('label') == search_payload:
                ids = []
                scores = []
                user_vec = [record.vector for record in user_vector][0]
                result = self.client.search(
                    collection_name=collection,
                    query_vector=user_vec,
                    query_filter=items_label,
                    limit=1000
                )

                for point in result:
                    ids.append(str(point.id))  
                    scores.append(point.score)
                response = {"content_id": ids, "recStrength": scores}
                return response
        except Exception as e:
            print(f"Error during search: {e}")
            traceback.print_exc()    
    
   
    def query(self):
        print(self.client.scroll("trend",with_vectors=True))

if __name__ == "__main__":


    qdrant = Qdrant()
    qdrant.query()
    
 

      
    
  