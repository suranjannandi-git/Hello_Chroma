import secrets
import binascii
import chromadb
import pandas as pd

vector_collection_name = "my_collection" #os.getenv("VECTOR_COLLECTION_NAME", None)
chroma_db_path = "./mnt" 
chroma_client = chromadb.PersistentClient(path=chroma_db_path)

def generate_hex_string(length):
    # Generate random bytes
    random_bytes = secrets.token_bytes(length // 2)  # Convert to bytes, dividing by 2 as 1 byte = 2 hex characters

    # Convert bytes to hexadecimal string
    hex_string = binascii.hexlify(random_bytes).decode('utf-8')

    return hex_string

def add_record(document, response, id="", collection_name=vector_collection_name):
    hex_id = id if id else str(generate_hex_string(24))
    target_collection = chroma_client.get_or_create_collection(collection_name)
    target_collection.upsert(documents=document, metadatas=[{'response': response}], ids=hex_id)
    return hex_id

def query_record(query_texts, num_results=7, collection_name=vector_collection_name):
    # collection_name = db_alias
    target_collection = chroma_client.get_collection(collection_name)
    result = target_collection.query(query_texts=query_texts, n_results=num_results)
    
    table_data = []
    for i in range(len(result['ids'][0])):
        table_data.append({            
            'document': result['documents'][0][i],            
            'response': result['metadatas'][0][i]['response'],
            'document_id': result['ids'][0][i],
            'distance': result['distances'][0][i]
        })

    df = pd.DataFrame(table_data)
    df = df.to_dict(orient='records')
    return df

def delete_record(id, collection_name=vector_collection_name):
    target_collection = chroma_client.get_collection(collection_name)
    target_collection.delete(ids=[id])
    return id

def drop_collection(collection_name=vector_collection_name):
    try:
        chroma_client.delete_collection(name=collection_name)
        print(f"Collection '{collection_name}' has been dropped successfully.")
    except Exception as e:
        print(f"An error occurred while dropping the collection: {e}")

def get_all_records(num_results = 100, collection_name=vector_collection_name):
    target_collection = chroma_client.get_or_create_collection(collection_name)
    result = target_collection.peek(limit=num_results)
    
    df = pd.DataFrame({            
        'document': result['documents'],
        'response': [meta['response'] for meta in result['metadatas']],
        'document_id': result['ids']
    })
    df = df.to_dict(orient='records')
    return df


if __name__=="__main__":
    COLLECTION_NAME = "my_collection"
    
    # # insert/edit records in collection
    print(add_record(collection_name=COLLECTION_NAME,document="How many records",response="Select * from Table"))
    print(add_record(collection_name=COLLECTION_NAME,document="Sales total",response="Select sum(net_sales) from Table"))
    print(add_record(collection_name=COLLECTION_NAME,document="Shayam's sales",response="Select sum(net_sales) from Table where name = 'Shayam'"))

    # show all the records in collection
    result = get_all_records(num_results=100, collection_name=COLLECTION_NAME)
    print("Get All Record", result)
        
    result = query_record(query_texts="Sales", num_results=3, collection_name=COLLECTION_NAME)
    # print("Query Record", result)
    for record in result:
        document = record.get('document')
        document_id = record.get('document_id')
        distance = record.get('distance')
        response = record.get('response')
        print(f"Document: {document}, ID: {document_id}, Distance: {distance}, Response: {response}")

    drop_collection(COLLECTION_NAME)
    
