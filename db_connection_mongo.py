#-------------------------------------------------------------------------
# AUTHOR: DAT NGUYEN
# FILENAME: db_connection_mongo.py
# SPECIFICATION: PyMongo command to handle operation on MongoDB database
# FOR: CS 5180- Assignment #2
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime
import string
from collections import defaultdict

def connectDataBase():
    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017
    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]
        return db

    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    terms = docText.lower().translate(str.maketrans('', '', string.punctuation)).split(" ")
    term_count = {}
    for term in terms:
        term_count[term] = term_count.get(term, 0) + 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    termArray = []
    totalNumchars = 0
    for term, count in term_count.items():
        termArray.append({"term": term, "count": count, "num_chars": len(term)})
        totalNumchars+= len(term)*count


    #Producing a final document as a dictionary including all the required fields
    # --> add your Python code here
    docDate = datetime.datetime.strptime(docDate, "%Y-%m-%d")
    document = {"_id": docId,
                "text": docText,
                "title": docTitle,
                "date": docDate,
                "category": docCat,
                "terms": termArray,
                "num_chars": totalNumchars,
                }

    # Insert the document
    # --> add your Python code here
    col.insert_one(document)


def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    col.delete_one({"_id": docId})

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here
    result = col.aggregate([{"$unwind": "$terms"},
    {
        "$project": {
            "_id": 0,
            "term": "$terms.term",
            "documents": {
                "$concat": [
                    "$title",
                    ":",
                    {"$toString": "$terms.count"}
                ]
            }
        }
    },
    {"$sort": {"term": 1}},
    ])
    termDict = defaultdict(list)
    for doc in result:
        termDict[doc['term']].append(doc['documents'])
    termDict = {term: ','.join(entries) for term, entries in termDict.items()}
    return termDict