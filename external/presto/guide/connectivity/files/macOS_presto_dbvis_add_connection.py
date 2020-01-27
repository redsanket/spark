import xml.etree.ElementTree as ET
from shutil import copyfile
from datetime import datetime
import os
import sys
from collections import OrderedDict


clusterdetails = {
    "XB": ("XandarBlue", "xandarblue-presto.blue.ygrid.yahoo.com"),
    "XT": ("XandarBlue", "xandartan-presto.tan.ygrid.yahoo.com")
}

if len(sys.argv) < 3:
    print """Please provide a valid cluster short name as an argument to the script.
    Syntax: python macOS_presto_dbvis_add_connection.py <cluster short name> <catalog name> [<schema name>]
	Eg: python macOS_presto_dbvis_add_connection.py XB jetblue benzene
	Schema name is an optional argument
	Valid cluster short names are {}
	Refer https://git.ouroath.com/pages/hadoop/docs/presto/deployment.html#ygrid""".format(
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


def add_attributes(sub_element, attributes_dict, label, tag):
    for key, value in attributes_dict.items():
        added_property = ET.SubElement(sub_element, label)
        added_property.text = str(value)
        added_property.set(tag, key)

dbVis_alias = "Presto {} {} mTLS"

dbVis_connection_details = """Alias = {}
Driver = Presto
Userid = {}
Profile = auto
Type = presto
ServerInfoFormat = 1"""

properties = """SSL = true
SSLCertificatePath = {}/.athenz/griduser.uid.{}.cert.pem
SSLKeyStorePath = {}/.athenz/griduser.uid.{}.key.pem
SSLTrustStorePath = {}/.athenz/yahoo_certificate_bundle.pem
SessionProperties = query_max_execution_time=15m
dbvis.ConnectionModeMigrated = true
dbvis.TransactionIsolation = 0"""

url_properties = """Server = {}
Port = 4443
Catalog = {}
Schema = {}"""

username = os.getenv('USER')
userdir = os.getenv('HOME')
cluster = sys.argv[1]
catalog = sys.argv[2]

schema = ""
if len(sys.argv) > 3:
    schema = sys.argv[3]

if cluster in clusterdetails:
    cluster_name = clusterdetails[cluster][0]
    host_name = clusterdetails[cluster][1]
    dbVis_alias = dbVis_alias.format(cluster_name, catalog)

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

        dbVis_connection_details = dbVis_connection_details.format(dbVis_alias, username)
        dbVis_connection_details_dict = OrderedDict(
            [i.strip() for i in item.split("=")] for item in dbVis_connection_details.split("\n"))

        properties = properties.format(userdir, username, userdir, username, userdir)
        properties_dict = OrderedDict(
            [i.strip() for i in item.split("=", 1)] for item in properties.split("\n"))

        url_properties = url_properties.format(host_name, catalog, schema)
        url_properties_dict = OrderedDict(
            [i.strip() for i in item.split("=")] for item in url_properties.split("\n"))

        if maxDatabaseId is None:
            for object in root.iter("Objects"):
                for databaseId in object.findall("Database"):
                    maxDatabaseId = max(maxDatabaseId, int(databaseId.get("id")))
                maxDatabaseId = str(maxDatabaseId + 1)
                ET.SubElement(object, "Database",  {'id': maxDatabaseId})

        # Add connection parameters
        added_database = ET.SubElement(databases, "Database", {'id': maxDatabaseId})
        for key, value in dbVis_connection_details_dict.items():
            keyTag = ET.SubElement(added_database, key)
            keyTag.text = value

        added_properties = ET.SubElement(added_database, "Properties")
        add_attributes(added_properties, properties_dict, "Property", "key")

        added_url_properties = ET.SubElement(added_database, "UrlVariables")
        added_driver = ET.SubElement(added_url_properties, "Driver")
        added_driver.text = "Presto"
        add_attributes(added_driver, url_properties_dict, "UrlVariable", "UrlVariableName")

    # Create backup
    copyfile("{}/.dbvis/config70/dbvis.xml".format(userdir), "{}/.dbvis/config70/dbvis.xml.backup.{}".format(userdir, datetime.now().strftime('%Y%m%d-%H%M%S')))

    # Modify original
    tree.write("{}/.dbvis/config70/dbvis.xml".format(userdir))

else:
    print """Invalid argument.
	Valid cluster short names are {}
	Refer https://git.ouroath.com/pages/hadoop/docs/presto/deployment.html#ygrid
	""".format(clusterdetails.keys())
