import xml.etree.ElementTree as ET
from shutil import copyfile
from datetime import datetime
import os
import sys
from collections import OrderedDict


clusterdetails = {
    "AR": ("AxoniteRed", "axonitered-hs2.red.ygrid.yahoo.com"),
    "BB": ("BassniumBlue", "bassniumblue-hs2.blue.ygrid.yahoo.com"),
    "BR": ("BassnniumRed", "bassnniumred-hs2.red.ygrid.yahoo.com"),
    "BT": ("BassniumTan", "bassniumtan-hs2.tan.ygrid.yahoo.com"),
    "DB": ("DilithiumBlue", "dilithiumblue-hs2.blue.ygrid.yahoo.com"),
    "DR": ("DilithiumRed", "dilithiumred-hs2.red.ygrid.yahoo.com"),
    "JB": ("JetBlue", "jetblue-hs2.blue.ygrid.yahoo.com"),
    "KR": ("KryptoniteRed", "kryptonitered-hs2.red.ygrid.yahoo.com"),
    "MR": ("MithrilRed", "mithrilred-hs2.red.ygrid.yahoo.com"),
    "OB": ("OxiumBlue", "oxiumblue-hs2.blue.ygrid.yahoo.com"),
    "PB": ("PhazonBlue", "phazonblue-hs2.blue.ygrid.yahoo.com"),
    "PR": ("PhazonRed", "phazonred-hs2.red.ygrid.yahoo.com"),
    "PT": ("PhazonTan", "phazontan-hs2.tan.ygrid.yahoo.com"),
    "TT": ("TiberiumTan", "tiberiumtan-hs2.tan.ygrid.yahoo.com"),
    "UB": ("UraniumBlue", "uraniumblue-hs2.blue.ygrid.yahoo.com"),
    "UT": ("UraniumTan", "uraniumtan-hs2.tan.ygrid.yahoo.com"),
    "ZT": ("ZaniumTan", "zaniumtan-hs2.tan.ygrid.yahoo.com")
}

if len(sys.argv) < 2:
    print """Please provide a valid cluster short name as an argument to the script.
    Syntax: python macOS_hive_dbvis_add_connection.py <cluster short name> [<schema/database name>]
	Eg: python macOS_hive_dbvis_add_connection.py JB benzene
	Schema/Database name is an optional argument
	Valid cluster short names are {}
	Refer https://git.ouroath.com/pages/hadoop/docs/hive/hiveserver2/index.html#hiveserver2-servers""".format(
        clusterdetails.keys())
    exit(0)

def yes_or_no(question):
    reply = str(raw_input(question+' (y/n): ')).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
    else:
        return yes_or_no("Please enter y or n\n{}".format(question))

dbVis_alias = "HS2 {} mTLS"
dbVis_connection_details = """Alias := {}
Url := jdbc:hive2://{}:4443/{};transportMode=http;httpPath=cliservice;ssl=true;sslTrustStore={}/.athenz/yahoo_certificate_bundle.jks;twoWay=true;sslKeyStore={}/.athenz/griduser.role.uid.{}.jks;keyStorePassword=changeit?tez.queue.name=default
Driver := Hive"""

username = os.getenv('USER')
userdir = os.getenv('HOME')
cluster = sys.argv[1]

schema = ""
if len(sys.argv) > 2:
    schema = sys.argv[2]

if cluster in clusterdetails:
    cluster_name = clusterdetails[cluster][0]
    host_name = clusterdetails[cluster][1]
    dbVis_alias = dbVis_alias.format(cluster_name)

    tree = ET.parse("{}/.dbvis/config70/dbvis.xml".format(userdir))
    root = tree.getroot()

    maxDatabaseId = None
    for databases in root.iter("Databases"):
        for database in databases.findall("Database"):
            if database.find("Alias").text == dbVis_alias:
                if yes_or_no("Do you want to overwrite the existing connection properties for {}".format(dbVis_alias)):
                    databases.remove(database)
                    for object in root.iter("Objects"):
                        for databaseId in object.findall("Database"):
                            if databaseId.get("id") == database.get("id"):
                                maxDatabaseId = databaseId.get("id")
                else:
                    exit(0)

        if maxDatabaseId is None:
            for object in root.iter("Objects"):
                for databaseId in object.findall("Database"):
                    maxDatabaseId = max(maxDatabaseId, int(databaseId.get("id")))
                maxDatabaseId = str(maxDatabaseId + 1)
                ET.SubElement(object, "Database",  {'id': maxDatabaseId})

        dbVis_connection_details = dbVis_connection_details.format(dbVis_alias, host_name, schema, userdir, userdir, username)

        dbVis_connection_details_dict = OrderedDict(
            [i.strip() for i in item.split(":=")] for item in dbVis_connection_details.split("\n"))

        # Add connection parameters
        added_database = ET.SubElement(databases, "Database", {'id': maxDatabaseId})
        for key, value in dbVis_connection_details_dict.items():
            keyTag = ET.SubElement(added_database, key)
            keyTag.text = value
    # Create backup
    copyfile("{}/.dbvis/config70/dbvis.xml".format(userdir), "{}/.dbvis/config70/dbvis.xml.backup.{}".format(userdir, datetime.now().strftime('%Y%m%d-%H%M%S')))
    # Modify original
    tree.write("{}/.dbvis/config70/dbvis.xml".format(userdir))

else:
    print """Invalid argument.
	Valid cluster short names are {}
	Refer https://git.ouroath.com/pages/hadoop/docs/hive/hiveserver2/index.html#hiveserver2-servers""".format(clusterdetails.keys())
