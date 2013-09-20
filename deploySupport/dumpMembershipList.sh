#!/bin/bash
#
# dumpMembershipList.sh - a glorified way to run "igor fetch -members 'grid_re.clusters.something'"
#
# We need to retrieve this data from a machine that does not want to run 'igor'
# So we pull it from the web API:
#     retrieve the URL "........./roles/v1/roles/grid_re.clusters.something/members"
# where the leading part is the igor server. ("yinst which-dist" provides that address.)
#
# The rest is just prying out the data from the XML:
#	(1) the XSLT program just extracts the 'name' attribute from each role, to <stdout>
#       (2) the tr and grep are formatting.
#
#
# argument:  cluster name or role name-fragment. (The argument is appended to 'grid_re.clusters.')
#
# returns:  the membership list of that igor role.
#
# failure:  If the role does not exist, it will tell you, but it really assumes you have already checked.
#

cluster=$1
[ -z "$cluster" ] && cluster=ankh
case $cluster in
    grid_re.*)
      export role=$1 ;;
    *)
      export role=grid_re.clusters.$1 ;;
esac

memberxml=/tmp/members.$$.xml
xsltprog=/tmp/members.$$.xsl
logfile=/tmp/members.$$.log
xslt=/usr/bin/xsltproc
igor=/home/y/bin/igor
 
status=0
if [ -e "$xslt" ]
then
    url=`/usr/local/bin/yinst  which-dist | grep http:`
    
    trap "rm -f $xsltprog    $memberxml $logfile ; exit \$status" 0
    cat > $xsltprog   <<'EOF'
<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="text"/>
<xsl:template match="configuration">
<xsl:for-each select="roles/role">
<xsl:value-of select="member"/>
</xsl:for-each>
</xsl:template>
</xsl:stylesheet>
EOF
    
    wget  -o $logfile      "$url/roles/v1/roles/$role/members" && 
      mv  members   $memberxml
    [ $? -ne 0 ] && status=$?
    
    [ -f $memberxml ] && xsltproc  $xsltprog      $memberxml | tr -d ' '|  egrep -v '^ *$'
    [ $? -ne 0 ] && status=$?
else
    $igor fetch -members $role
fi
