from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd


class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """
 # Retrieve all records from the MongoDB collection
    
    def __init__(self, user, password):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # You must edit the connection variables below to reflect
        # your own instance of MongoDB!
        #
        # Connection Variables
        #
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 32073
        DB = 'AAC'
        COL = 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (user, password, HOST, PORT))
        self.database = self.client[DB]
        self.collection = self.database[COL]

    def create(self, data):
        if data is not None:
            self.collection.insert_one(data)  # data should be a dictionary            
        else:
            raise Exception("Nothing to save because the data parameter is empty")

    def read(self, filter):
        # Implement your read logic here
        # For example, you can use self.collection.find(filter) to retrieve data
        return list(self.collection.find(filter))

    def update(self, query, update_data):
        if query and update_data:
            try:
                result = self.collection.update_many(query, {"$set": update_data})
                return result.modified_count
            except Exception as e:
                print(f"Error updating documents: {e}")
                return 0
        else:
            raise Exception("Both query and update_data parameters are required")

    def delete(self, query):
        if query:
            try:
                result = self.collection.delete_many(query)
                return result.deleted_count
            except Exception as e:
                print(f"Error deleting documents: {e}")
                return 0
   


# Example usage:
if __name__ == "__main__":
    USER = 'aacuser'
    PASS = 'password'

    shelter = AnimalShelter(USER, PASS)

    testCreate = shelter.create({"name": "Shocker", "species": "Dog", "breed": "Jack Russell"})
    print("Create Result:", testCreate)

    testRead = shelter.read({"name": "Shocker"})
    print("Read Result:", testRead)

    update_query = {"name": "Shocker"}
    update_data = {"breed": "Labrador"}
    shelter.update(update_query, update_data)

    
    delete_query = {"species": "Dog"}
    shelter.delete(delete_query)
     
    