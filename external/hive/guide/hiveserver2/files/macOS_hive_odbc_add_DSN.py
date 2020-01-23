import configparser
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
    print """Please provide a valid cluster short name  as an argument to the script.
    Syntax: python macOS_hive_odbc_add_DSN.py <cluster short name> [<schema/database name>]
	Eg: python macOS_hive_odbc_add_DSN.py JB benzene
	Schema/Database name is an optional argument
	Valid cluster short names are {}
	Refer https://git.ouroath.com/pages/hadoop/docs/hive/hiveserver2/index.html#hiveserver2-servers""".format(
        clusterdetails.keys())
    exit(0)

odbc_data_source = "HS2 {} mTLS"
odbc_data_source_details = """Driver = /Library/simba/hiveodbc/lib/libhiveodbc_sbu.dylib
Description          = {} HS2 mTLS
Host                 = {}
Port                 = 4443
HiveServerType       = 2
ThriftTransport      = 2
SSL                  = 1
TwoWaySSL            = 1
AuthMech             = 0
HTTPPath             = cliservice
ClientCert           = {}/.athenz/griduser.uid.{}.cert.pem
ClientPrivateKey     = {}/.athenz/griduser.uid.{}.key.pem
Schema               = {}
user                 = {}
UseNativeQuery       = 1
FastSQLPrepare       = 1"""

def yes_or_no(question):
    reply = str(raw_input(question+' (y/n): ')).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
    else:
        return yes_or_no("Please enter y or n\n{}".format(question))

username = os.getenv('USER')
userdir = os.getenv('HOME')
cluster = sys.argv[1]

schema = ""
if len(sys.argv) > 2:
    schema = sys.argv[2]

if cluster in clusterdetails:
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read("{}/.odbc.ini".format(userdir))

    host_name = clusterdetails[cluster][1]
    odbc_data_source_cluster = odbc_data_source.format(clusterdetails[cluster][0])

    if odbc_data_source_cluster in config["ODBC Data Sources"]:
        if yes_or_no("Do you want to overwrite the existing connection properties for {}".format(odbc_data_source_cluster)):
            config.remove_option("ODBC Data Sources", odbc_data_source_cluster)
            config.remove_section(odbc_data_source_cluster);
        else:
            exit(0)

    config.set("ODBC Data Sources", odbc_data_source_cluster, "Simba Hive ODBC Driver")
    odbc_data_source_details_cluster = odbc_data_source_details.format(clusterdetails[cluster][0],
                                                                       host_name, userdir, username,
                                                                       userdir, username, schema, username)
    config.add_section(odbc_data_source_cluster)
    odbc_data_source_details_cluster_dict = OrderedDict(
        [i.strip() for i in item.split("=")] for item in odbc_data_source_details_cluster.split("\n"))
    config[odbc_data_source_cluster] = odbc_data_source_details_cluster_dict

    # Create backup
    copyfile("{}/.odbc.ini".format(userdir), "{}/.odbc.ini.backup.{}".format(userdir, datetime.now().strftime('%Y%m%d-%H%M%S')))

    # Modify original
    with open("{}/.odbc.ini".format(userdir), 'w') as configfile:
        config.write(configfile)
        configfile.close()

else:
    print """Invalid argument.
	Valid cluster short names are {}
	Refer https://git.ouroath.com/pages/hadoop/docs/hive/hiveserver2/index.html#hiveserver2-servers""".format(clusterdetails.keys())
