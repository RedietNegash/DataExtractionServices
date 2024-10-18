from qdrant_client import QdrantClient
from qdrant_client.http import models
import os
import traceback

class Qdrant:

    def __init__(self):

        try:
            vector_db = os.getenv('VECTOR_DATABASE','qdrant')
            self.client = QdrantClient(host=vector_db)
            print(f"qdrant connected")
        except:
            print('qdrant connection is failed')

    def get_create_collection(self,collection_name, vector_size): 
        
        try:
            # Try to get the existing collection
            self.client.get_collection(collection_name)
        except: 
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.DOT) )
            print(f"Collection '{collection_name}' CREATED.")


    def batch_insert_data(self, collection_name,payloads, data, vector_size):
        try:
            data['id'] = list(map(int, data['id']))
            
            payload = []
            for i in range(len(data['id'])):
                payload.append(
                {
                "label":payloads
                })

            self.get_create_collection(collection_name, vector_size)
            self.client.upsert(
                            collection_name=collection_name,
                            points=models.Batch(
                                ids=data['id'],
                                vectors=data['embedding'],
                                payloads=payload
                            ))
            print(f"vectors saved to the {collection_name} collection")
        except:
            traceback.print_exc()
    
    def query_and_search(self, user_id, search_payload, query_payload, collection):
        
        try:
            items_label = models.Filter(must=[models.FieldCondition(key="label", match=models.MatchValue(value=query_payload))])
            
            user_vector = self.client.retrieve(collection_name=collection, ids=[user_id], with_vectors=True,)
            
            if user_vector[0].payload.get('label') == search_payload:
                ids=[]
                scores = []
                user_vec = [record.vector for record in user_vector][0]
                result = self.client.search(
                        collection_name=collection,
                        query_vector=user_vec,
                        query_filter=items_label,
                        limit=1000)

                for point in result:
                    ids.append(str(point.id))  
                    scores.append(point.score)
                response = {"content_id": ids, "recStrength": scores}
                return response
        
        except:
            traceback.print_exc()
