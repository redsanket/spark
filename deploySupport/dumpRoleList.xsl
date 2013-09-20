<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="text"/>
<xsl:template match="/" >


<xsl:for-each select="roles/namespace/role">

role: <xsl:value-of select="@ns"/>.<xsl:value-of select="@name"/>

</xsl:for-each>
</xsl:template>
</xsl:stylesheet>

