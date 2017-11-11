from lxml import etree
from datetime import datetime
import settings
from mongoRepository import mongoRep

import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--path', help='absolute path for comments.xml')
args = parser.parse_args()

filePath= args.path

settingsData = settings.get()

rep = mongoRep(settingsData["connectionString"], "comments")

buffer = []
bufferLength = 1000
i = 1
context = etree.iterparse(filePath, events=('end',), tag='row')
 
for event, elem in context:
    doc = {"_id": int(elem.attrib["Id"])}
    if "UserId" in elem.attrib:
        doc["user"] = int(elem.attrib["UserId"])
    doc["post"] = int(elem.attrib["PostId"])
    doc["date"] = datetime.strptime(str(elem.attrib["CreationDate"]), "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)
    doc["text"] = str(elem.attrib["Text"])
    doc["score"]= int(elem.attrib["Score"])
    buffer.append(doc)
    if(len(buffer) == bufferLength):
        rep.insert_many(buffer)
        print ("Inserted: {0} docs".format(i * bufferLength))
        i = i + 1
        buffer = []
    
if(len(buffer) != 0):
    rep.insert_many(buffer)
    print ("Inserted: {0} docs".format((i-1) * bufferLength + len(buffer))