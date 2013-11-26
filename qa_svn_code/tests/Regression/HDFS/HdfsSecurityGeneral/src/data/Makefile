
OTOP_DIR=/tmp/PermTest
getlist:	
	(cd /tmp/PermTest; find . -type f ) | sort > files.txt
	(cd /tmp/PermTest; find . -type d ) | grep Dir | sort > dirs.txt 

chFileOwner:
	(cd $TOP_DIR; chown -R hadoopqa * ) 

	
	
