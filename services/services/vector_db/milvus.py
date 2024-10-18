from pymilvus import Collection, connections, FieldSchema,CollectionSchema, FieldSchema, DataType,utility
from recommender.utils.vector_db.vector_db_interface import VectoryDBInterface
import os
import traceback
import time
import math

EMBEDDING_FIELD="embedding"

DEFAULT_SCHEMA = [FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=100,is_primary=True),FieldSchema(name=EMBEDDING_FIELD, dtype=DataType.FLOAT_VECTOR, dim=96)]



class MivlusDB(VectoryDBInterface):

    def __init__(self):
        try:
            self.host = os.getenv('MILVUS_HOST', 'standalone')
            self.port = os.getenv('MILVUS_PORT', 19530)
            self.search_params ={
                "metric_type": "IP",
                "params": {"nprobe": 10},}
            self.conn = connections.connect(host=self.host, port=self.port)
        except:
            traceback.print_exc() 
            raise Exception("Milvus connection failed")



    def _create_collection(self,collection_name,schema=None):
        if schema is None:
            schema = CollectionSchema(DEFAULT_SCHEMA)
        else:
            schema = CollectionSchema(schema)
        try:
            collection = Collection(collection_name, schema)
            return collection        
        except:
                traceback.print_exc()

            

    def insert_batch(self,schema_name, data):
        try:
           collection_name = f"{schema_name}_{int(time.time() * 1000)}"
           collection = self._create_collection(collection_name)
        #    index creation needs some abstaraction, it is dependent on the data['id] attribute, so data arg is needed to contain ids as a constraint

           collection.insert([data['id'], data['embedding']])
           collection.create_index(field_name=EMBEDDING_FIELD, index_params={"index_type": "IVF_FLAT","metric_type": "L2","params": {"nlist": int(4*math.sqrt(len(data['id'])))}, }) 
           collection.load()

           if not utility.has_collection(f"{schema_name}_alias"):
                self._create_alias(collection_name,f"{schema_name}_alias")
                return {"created":True,f"collection_name":collection_name}
            
           return {"created":False,f"collection_name":collection_name}

        except:
            traceback.print_exc()

    def update(self,schema_name,collection_name):
        try:
            alias = f"{schema_name}_alias"
            for collect in utility.list_collections():
                collection = Collection(collect)
                if alias in collection.aliases:  
                    self._alter_alias(collection_name,alias)
                    collection.drop()
                    break
            return 
            
        except:

            traceback.print_exc()


    def _create_alias(self, collection_name, alias_name):
        try:
            utility.create_alias(collection_name,alias_name)
            return
        except:
            traceback.print_exc()

    def _alter_alias(self, collection_name, alias_name):
        try:
            utility.alter_alias(collection_name,alias_name)

            return 
        except:
            traceback.print_exc()

    def _drop_alias(self, alias_name):
        try:
            alias = utility.drop_alias(alias_name)
            return 
        except:
            traceback.print_exc()   

    def query(self, schema, query, top_k=10):
        try:
            collection = Collection(f"{schema}_alias")
            res = collection.query(expr='id=="' + str(query)+ '"', output_fields=["embedding"])
            return res
        except:
            traceback.print_exc()


    def search(self, schema, query, top_k=10):
        try:
            collection = Collection(f"{schema}_alias")
            res = collection.search([query],"embedding",self.search_params,limit=top_k, output_fields=['id'])
            return res
        except:
            traceback.print_exc()



    def query_and_search(self, query_schema,search_schema, query, top_k=1000):
        try:
            ids=[]
            scores = []
            querry_collection = Collection(f"{query_schema}_alias")
            search_collection = Collection(f"{search_schema}_alias")
            query_response  = querry_collection.query(expr='id=="' + str(query)+ '"', output_fields=["embedding"])
            search_response = search_collection.search([query_response[0]["embedding"]],"embedding",self.search_params,limit=top_k, output_fields=['id'])

            for hits in search_response:
                for result in hits:
                    ids.append(result.id)
                    scores.append(result.distance)
            response = {"content_id":ids,"recStrength":scores}
            return response
        except:
            traceback.print_exc()
