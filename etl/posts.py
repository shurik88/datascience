from lxml import etree
from datetime import datetime
import settings
from mongoRepository import mongoRep

import argparse
from parse import findall
parser = argparse.ArgumentParser()
parser.add_argument('--path', help='absolute path for Posts.xml')
args = parser.parse_args()

filePath= args.path

settingsData = settings.get()
tagsSearchPattern = "<{}>"
rep = mongoRep(settingsData["connectionString"], "posts")
context = etree.iterparse(filePath, events=('end',), tag='row')
buffer = []
bufferLength = 1000
i = 1
for event, elem in context:
    # print (elem.attrib)
    # if("Tags" in elem.attrib):
    #     print([r.fixed[0] for r in findall(tagsSearchPattern, str(elem.attrib["Tags"]))])
    # if (i == 5):
    #     break
    # i = i + 1
    doc = {"_id": int(elem.attrib["Id"])}
    doc["type"] = int(elem.attrib["PostTypeId"])
    if("AcceptedAnswerId" in elem.attrib):
        doc["accepted"] = int(elem.attrib["AcceptedAnswerId"])
    if("ParentId" in elem.attrib):
        doc["par"] = int(elem.attrib["ParentId"])
    doc["date"] = datetime.strptime(str(elem.attrib["CreationDate"]), "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)
    doc["score"] = int(elem.attrib["Score"])
    if("ViewCount" in elem.attrib):
        doc["view"] = int(elem.attrib["ViewCount"])
    else:
        doc["view"] = 0
    doc["body"] = str(elem.attrib["Body"])
    if("OwnerUserId" in elem.attrib):
       doc["owner"] = int(elem.attrib["OwnerUserId"])
    
    if ("Title" in elem.attrib):
        doc["title"] = str(elem.attrib["Title"])
    if("Tags" in elem.attrib):
        doc["tags"]=[r.fixed[0] for r in findall(tagsSearchPattern, str(elem.attrib["Tags"]))]
    buffer.append(doc)
    if(len(buffer) == bufferLength):
        rep.insert_many(buffer)
        print ("Inserted: {0} docs".format(i * bufferLength))
        i = i + 1
        buffer = []
    
if(len(buffer) != 0):
    rep.insert_many(buffer)
    print ("Inserted: {0} docs".format((i - 1) * bufferLength + len(buffer)))