
# Permutation of the following:
#	users: hadoopqa, hadooopqa
#	group: users, hdfs
# 	dir permission:  700, 740, 744, 764, 766, 777
# 	file permission: 400, 440, 644, 664, 660, 666


SUDO=""
PERM_SRC_DIR="/homes/cwchung/svn/SG/src/data"
export HDFS_TOP_DIR=/tmp/PermTest2

# list of directory and file name, relative to HDFS_TOP_DIR
PERM_DIRLIST="$PERM_SRC_DIR/perm-dirlist.txt"
PERM_FILELIST="$PERM_SRC_DIR/perm-filelist.txt"
PERM_FILENAME="$PERM_SRC_DIR/perm-fname.txt"		# just the filename here
PERM_DATAFILE="$PERM_SRC_DIR/smallFile14Byte"		# 

POWNERS="hadoopqa  hdfsqa"
PGROUPS="users  hdfs"
PDIR_PERM="700 740 744 764 766 777"
PFILE_PERM="400  440  644  664  660  666"


function genFileList {
	for dir_owner in $POWNERS ; do
   	    for dir_group in $PGROUPS ; do
		for dir_perm in $PDIR_PERM ; do
		   DirName=${dir_owner}-${dir_group}-Dir-${dir_perm}
		   #echo $DirName 

			for owner in $POWNERS ; do
			    for group in $PGROUPS ; do
				for fperm in $PFILE_PERM ; do
				    FileName=${owner}-${group}-File-${fperm}
				    echo $DirName/$FileName 
				done
			    done
			done


		done
	    done
	done
}

# This function make use of the dirlist.txt and fname.txt  and is confirmed to generate the same output as genFileList
function genFileList2 {
    for Dir in `cat $PERM_DIRLIST` ; do
        for File in `cat $PERM_FILENAME` ; do
	    FileName=$Dir/$File
	    echo $FileName
	done
    done
}

function genFileNames {
	for owner in $POWNERS ; do
   	    for group in $PGROUPS ; do
	        for fperm in $PFILE_PERM ; do
		    FileName=${owner}-${group}-File-${fperm}
		    echo $FileName 
	        done
	    done
	done

}

function genDirList {
	for dir_owner in $POWNERS ; do
   	    for dir_group in $PGROUPS ; do
		for dir_perm in $PDIR_PERM ; do
			DirName=${dir_owner}-${dir_group}-Dir-${dir_perm}
			echo $DirName 
		done
	    done
	done
}


function rmHdfsDirs {
	DIRS=`cat $PERM_DIRLIST | sed -e "s#^#${HDFS_TOP_DIR}/#" `
	echo hadoop dfs -rmr  $DIRS
	hadoop dfs -rmr  $DIRS
}


function mkHdfsDirs {
	
	DIRS=`cat $PERM_DIRLIST | sed -e "s#^#${HDFS_TOP_DIR}/#" `
	echo hadoop dfs -mkdir -p $DIRS
	hadoop dfs -mkdir -p $DIRS
}

function mkHdfsFiles {
	DATA_FILE=

	FILES=`cat $PERM_FILELIST | sed -e "s#^#${HDFS_TOP_DIR}/#" `
	for File in $FILES ; do
		echo hadoop dfs -copyFromLocal $PERM_DATAFILE $File
		hadoop dfs -copyFromLocal $PERM_DATAFILE $File
	done

}

function updateHdfsFileOwner {

	FILES=`cat $PERM_FILELIST | sed -e "s#^#${HDFS_TOP_DIR}/#" `
	for File in $FILES ; do
		echo hadoop dfs -copyFromLocal $PERM_DATAFILE $File
		hadoop dfs -copyFromLocal $PERM_DATAFILE $File
	done

}

function changeHdfsDirOwner {

	cat dirs.txt | egrep "hdfsqa-users-File|hdfsqa-hdfs-File"
	echo sudo -u hdfs hadoop fs -chown hdfsqa ` cat dirs.txt | fgrep "hdfsqa" | sed -e "s#^#$TOP_DIR#" `
	sudo -u hdfs hadoop fs -chown hdfsqa ` cat dirs.txt | fgrep "hdfsqa" | sed -e "s#^#$TOP_DIR#" `

	echo sudo -u hdfs hadoop fs -chgrp users ` cat dirs.txt | fgrep "users" | sed -e "s#^#$TOP_DIR#" `
	sudo -u hdfs hadoop fs -chgrp users ` cat dirs.txt | fgrep "users" | sed -e "s#^#$TOP_DIR#" `

	return 0
}


function changeHdfsFileOwner {

	cat files.txt | egrep "hdfsqa-users-File|hdfsqa-hdfs-File"
	#echo sudo -u hdfs hadoop fs -chown hdfsqa ` cat files.txt | egrep "hdfsqa-users-File|hdfsqa-hdfs-File" | sed -e "s#^#$TOP_DIR#" `
	#sudo -u hdfs hadoop fs -chown hdfsqa ` cat files.txt | egrep "hdfsqa-users-File|hdfsqa-hdfs-File" | sed -e "s#^#$TOP_DIR#" `

	echo sudo -u hdfs hadoop fs -chgrp users ` cat files.txt | fgrep "users-File" | sed -e "s#^#$TOP_DIR#" `
	sudo -u hdfs hadoop fs -chgrp users ` cat files.txt | fgrep "users-File" | sed -e "s#^#$TOP_DIR#" `

	return 0
}


function changeHdfsDirPerm {
	for P in 700 744 766 776 777 ; do
	    echo $P 
	    hadoop fs -chmod $P  `fgrep File-$P dirs.txt | sed "s#^#$TOP_DIR#" `
	done
}


function changeHdfsFilePerm {
	for P in 400 440 600 640 644 666 ; do
	    echo $P 
	    # fgrep File-$P files.txt
	    hadoop fs -chmod $P  `fgrep File-$P files.txt | sed "s#^#$TOP_DIR#" `
	done
}


# test to record expected pass and expected erros
testHdfs() {
	echo "hadoop fs -lsr /user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2  2> perm-lsr.err  1>perm-lsr.out"
	hadoop fs -lsr /user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2  2> perm-lsr.err  1>perm-lsr.out
	echo 'hadoop fs -cat ` cat files.txt | sed -e \"s#^#$TOP_DIR/#\"  2 perm-cat.err 1perm-cat.out'
	hadoop fs -cat ` cat files.txt | sed -e "s#^#$TOP_DIR/#" ` 2> perm-cat.err 1>perm-cat.out

}


RDFILE=smallRDFile-755	
WRFILE=smallWRFile-755


setHdfs() {
	# first, create local dir
	TOP_DIR=/user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest/
	HADOOP=hadoop
	SUDO="sudo"
	CHMOD="chmod"
	CHOWN="chown"
	CHGRP="chgrp"
	MKDIR="mkdir"
	CP=cp
	LS=ls

	SUDO_OPT="hdfs"
	HADOOPCMD_OPT="fs"
	LS_OPT="-ld"
	MKDIR_OPT="-p"
	CHMOD_OPT=""
	CHOWN_OPT=""
	CHGRP_OPT=""

}


function mkHdfs {
	# now create the HDFS dir
	export TOP_DIR=/user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2/
	setHdfs

	which hadoop
	createHdfsDirs
	createHdfsFiles
	changeHdfsFilePerm
	changeHdfsFileOwner
	changeHdfsDirPerm
	changeHdfsDirOwner
	hadoop fs -lsr $TOP_DIR
}

function getList {
	# genDirList 	
	# genFileNames	
	# genFileList	

	genDirList 	| sort > $PERM_DIRLIST
	genFileNames	| sort > $PERM_FILENAME
	genFileList	| sort > $PERM_FILELIST
	####### genFileList2	| sort > xx	# as confirmation
}

####### Now actually generating the dir and file

################ Done #########
### genList
### mkHdfsDirs
### mkHdfsFiles
changeFilePerm
changeFileOwner
changeDirPerm
changeDirOwner

#echo To create HDFS Files
# changeFileOwner
# changeDirOwner
