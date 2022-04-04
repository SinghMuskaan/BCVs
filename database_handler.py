from pymongo import MongoClient


def update_values(process_code: str, processing_status):
    
    status = False
    try:
        conn = MongoClient()
        status = True
        print("Connected successfully!!!")
    except:
        status = False  
        print("Could not connect to MongoDB")
        
    client = MongoClient("mongodb+srv://Majorcms:Majorcms@khoj.nqwbp.mongodb.net/khoj?retryWrites=true&w=majority")
    db = client.MajorCMS
    collection = db.MajorCMS
    myquery = { "process_code": process_code }
    newvalues = { "$set": { "processing_status": processing_status} }
    res = collection.update_one(myquery, newvalues)        

    
    return status