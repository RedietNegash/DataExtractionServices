
from recommender.utils.vector_db.milvus import *

vector_db_type = os.getenv('VECTOR_DB','MILVUS')
def get_vectordb_instance():
    try:

        vector_db = None
        if vector_db_type == "MILVUS":
            print('setting up milvus')
            vector_db = MivlusDB()

            print('milvus setup done')
        return vector_db


    except:
        traceback.print_exc()
        pass

    