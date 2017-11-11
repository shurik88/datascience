#import xml.etree.ElementTree as ET
from lxml import etree
from datetime import datetime
import settings
from mongoRepository import mongoRep

import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--path', help='absolute path for users.xml')
args = parser.parse_args()

filePath= args.path

settingsData = settings.get()

rep = mongoRep(settingsData["connectionString"], "users")

context = etree.iterparse(filePath, events=('end',), tag='row')
 
for event, elem in context:
    doc = {"_id": int(elem.attrib["Id"])}
    if "Age" in elem.attrib:
        doc["age"] = int(elem.attrib["Age"])
    doc["name"] = str(elem.attrib["DisplayName"])
    doc["up"] = int(elem.attrib["UpVotes"])
    doc["down"] = int(elem.attrib["DownVotes"])
    doc["rep"] = int(elem.attrib["Reputation"])
    doc["accId"] = int(elem.attrib["AccountId"])
    doc["date"] = datetime.strptime(str(elem.attrib["CreationDate"]), "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)

    print (rep.insert(doc))