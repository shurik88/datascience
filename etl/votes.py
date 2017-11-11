from lxml import etree
from datetime import datetime
import settings
from mongoRepository import mongoRep

import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--path', help='absolute path for votes.xml')
args = parser.parse_args()

filePath= args.path

settingsData = settings.get()

rep = mongoRep(settingsData["connectionString"], "votes")

context = etree.iterparse(filePath, events=('end',), tag='row')
 
for event, elem in context:
    doc = {"_id": int(elem.attrib["Id"])}
    doc["type"] = int(elem.attrib["VoteTypeId"])
    doc["post"] = int(elem.attrib["PostId"])
    if "UserId" in elem.attrib:
        doc["user"] = int(elem.attrib["UserId"])
    doc["date"] = datetime.strptime(str(elem.attrib["CreationDate"]), "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)
    print (rep.insert(doc))