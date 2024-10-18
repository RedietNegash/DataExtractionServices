from qdrant_client import QdrantClient

# Initialize Qdrant client
client = QdrantClient(host='localhost', port=6333)

# List all collections
collections = client.get_collections()
print(collections)
