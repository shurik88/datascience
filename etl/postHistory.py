from lxml import etree
from datetime import datetime
import settings
from mongoRepository import mongoRep
import uuid

import argparse
from parse import findall
parser = argparse.ArgumentParser()
parser.add_argument('--path', help='absolute path for PostHistory.xml')
args = parser.parse_args()

filePath= args.path

settingsData = settings.get()
tagsSearchPattern = "<{}>"
rep = mongoRep(settingsData["connectionString"], "history")
context = etree.iterparse(filePath, events=('end',), tag='row')
buffer = []
bufferLength = 1000
i = 1
for event, elem in context:
    # print (elem.attrib)
    # if("Tags" in elem.attrib):
    #     print([r.fixed[0] for r in findall(tagsSearchPattern, str(elem.attrib["Tags"]))])
    # if (i == 10):
    #     break
    # i = i + 1
    doc = {"_id": int(elem.attrib["Id"])}
    doc["history"] = int(elem.attrib["PostHistoryTypeId"])
    doc["post"] = int(elem.attrib["PostId"])
    doc["rev"] = str(elem.attrib["RevisionGUID"])
    if "UserId" in elem.attrib:
        doc["user"] = int(elem.attrib["UserId"])
    if "Text" in elem.attrib:
        doc["text"] = str(elem.attrib["Text"])
    
    buffer.append(doc)
    if(len(buffer) == bufferLength):
        rep.insert_many(buffer)
        print ("Inserted: {0} docs".format(i * bufferLength))
        i = i + 1
        buffer = []
    
if(len(buffer) != 0):
    rep.insert_many(buffer)
    print ("Inserted: {0} docs".format((i - 1) * bufferLength + len(buffer)))