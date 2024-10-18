from abc import ABC, abstractmethod
class VectoryDBInterface(ABC):
    
    @abstractmethod
    def query(schema_name,query,top_k=10):
        pass

    # @abstractmethod
    # def insert():
    #     pass

    @abstractmethod
    def update():
        pass

    @abstractmethod
    def insert_batch(schema_name,id,embedding):
        pass


    @abstractmethod
    def search(schema_name,query,top_k=10):
        pass


    @abstractmethod
    def query_and_search(query_schema_name,search_schema_name,query,top_k=10):
        pass

    # @abstractmethod
    # def create_schema():
    #     pass


    # @abstractmethod
    # def update_or_create():
    #     pass

    

