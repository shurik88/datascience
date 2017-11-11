from pymongo import MongoClient

class mongoRep:
    def __init__(self, connectionString, colName):
        self.colName = str(colName)
        self.client = MongoClient(connectionString)
        self.db = self.client.get_default_database()
    def insert(self, doc):
        return self.db[self.colName].insert_one(doc).inserted_id
    def insert_many(self, docs):
        return self.db[self.colName].insert_many(docs).inserted_ids
