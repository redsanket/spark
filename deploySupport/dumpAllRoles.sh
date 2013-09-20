#!/bin/bash
#
# dumpAllRoles.sh - a glorified way to run "igor list -roles 'grid_re.*'"
#
# We need to retrieve this data from a machine that does not want to run 'igor'
# So we pull it from the web API: retrieve the URL "........./roles/v1/namespaces/grid_re/roles"
# where the leading part is the igor server. ("yinst which-dist" provides that address.)
#
# The rest is just prying out the data from the XML:
#	(1) the XSLT program just extracts the 'name' attribute from each role, to <stdout>
#       (2) the tr and grep are formatting.
#
#
#
#
#
#
#
#
#

rolelistxml=/tmp/rolelist.$$.xml
xsltprog=/tmp/rolelist.$$.xsl
logfile=/tmp/rolelist.$$.log
url=`/usr/local/bin/yinst  which-dist | grep http:`
xslt=/usr/bin/xsltproc
igor=/home/y/bin/igor

status=0
if [ -e "$xslt" ]
then
    trap "rm -f $xsltprog $rolelistxml $logfile ; exit \$status" 0
    
    cat > $xsltprog   <<'EOF'
<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="text"/>
<xsl:template match="/" >


<xsl:for-each select="roles/namespace/role">

<xsl:value-of select="@ns"/>.<xsl:value-of select="@name"/><xsl:text>
</xsl:text>

</xsl:for-each>
</xsl:template>
</xsl:stylesheet>
    
EOF
    
    cd /tmp
    wget -o $logfile     "$url/roles/v1/namespaces/grid_re/roles" &&
       mv roles  $rolelistxml
    [ $? -ne 0 ] && status=$?
    [ -f $rolelistxml ] && $xslt  $xsltprog      $rolelistxml 
    [ $? -ne 0 ] && status=$?
else
    $igor  list -roles "grid_re.*"
fi
