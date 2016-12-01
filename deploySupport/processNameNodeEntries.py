import getopt
import sys

def getlines(filename = None):
    if filename is not None:
       fd = open(filename, 'r')
       lns = fd.readlines()
       return [x.rstrip() for x in lns]
    return None

optlist, args = getopt.getopt(sys.argv[1:], 'o:1:2:')
namenodes = []
secondaryNNs = []
outfile = "/dev/stdout"
for (opt,val) in optlist:
   if opt == '-o':	outfile = val
   if opt == '-1':	namenodes = getlines(val)
   if opt == '-2':	secondaryNNs = getlines(val)
  

if len(namenodes) != len(secondaryNNs):
	print "Not the same number of secondary as primary NN"
l = []
for i in range(0,len(namenodes)):
    s = None
    if i in range(0,len(namenodes)):
        s = secondaryNNs[i]
    l.append([namenodes[i], s])

shortnames = [ x.split('.')[0] for x in namenodes]

# print l
# print shortnames

fd = open(outfile, 'w')
fd.writelines('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration xmlns:xi="http://www.w3.org/2001/XInclude">

<property>
<name>dfs.namenode.kerberos.principal</name>
<value>hdfs/_HOST@DEV.YGRID.YAHOO.COM</value>
<description>
Kerberos principal name for the NameNode
</description>
</property>

<property>
<name>dfs.secondary.namenode.kerberos.principal</name>
<value>hdfs/_HOST@DEV.YGRID.YAHOO.COM</value>
    <description>
        Kerberos principal name for the secondary NameNode.
    </description>
  </property>

<property>    
<name>dfs.namenode.kerberos.https.principal</name>
    <value>host/_HOST@DEV.YGRID.YAHOO.COM</value>
     <description>The Kerberos principal for the host that the NameNode runs on.
</description>

</property>

<property>
    <name>dfs.secondary.namenode.kerberos.https.principal</name>
    <value>host/_HOST@DEV.YGRID.YAHOO.COM</value>
    <description>The Kerberos principal for the hostthat the secondary NameNode 
runs on.</description>

</property>
''')


for (namenode,s) in l:
    nameserviceID = namenode.split('.')[0]
    fd.writelines('''<property>
    <name>dfs.namenode.rpc-address.%s</name>
      <value>%s:8020</value>
      <description>Namenode rpc address</description>
    </property>
    <property>
    <name>dfs.namenode.http-address.%s</name>
      <value>%s:50070</value>
      <description>http address </description>
    </property>
    <property>
    <name>dfs.namenode.https-address.%s</name>
      <value>%s:50470</value>
      <description>https address </description>
    </property>
'''  % ( nameserviceID, namenode, nameserviceID, namenode,nameserviceID, namenode))

    s_shortname = None
    if s is None:
        s_shortname = "unspecified"
    else:
        s_shortname = s.split('.')[0]
    fd.writelines('''
<property>
    <name>dfs.namenode.keytab.file.%s</name>
    <value>/etc/grid-keytabs/%s.dev.service.keytab</value>
    <description>
    Combined keytab file containing the namenode service and host principals.
    </description>
  </property>

  <property>
    <name>dfs.secondary.namenode.keytab.file.%s</name>
    <value>/etc/grid-keytabs/%s.dev.service.keytab</value>
  <description>
        Combined keytab file containing the namenode service and host principals.
    </description>
  </property>
'''  % ( nameserviceID, nameserviceID, nameserviceID, s_shortname))
    fd.writelines('''
 <property>
    <!-- cluster variant -->
    <name>dfs.namenode.secondary.http-address.%s</name>
    <value>%s:50090</value>
    <description>Address of secondary namenode web server</description>
  </property>

'''  % ( nameserviceID, s))

fd.writelines('''</configuration>''')
fd.close()
