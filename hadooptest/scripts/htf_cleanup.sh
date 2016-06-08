# This is the HTF's cleaning script, after the execution /homes/hadoopqa/tmp/hadooptest folder is created, but does not
# cleaned up. This script will remove all the folders and files under /homes/hadoopqa/tmp/hadooptest/*
# Which is older than 10 days.

CLEAN_UP_PATH="/homes/hadoopqa/tmp/hadooptest"
echo "CLEAN_UP_PATH  $CLEAN_UP_PATH"
if [ -d $CLEAN_UP_PATH ]
then
  echo "directory exists, cleaning..."
  dirCount=`ls -ltr $CLEAN_UP_PATH/* | wc -l`
  echo "no of directories under $CLEAN_UP_PATH  = $dirCount"
  if [ $dirCount -gt 0 ]
  then
     #isDirDeleted=`rm -rf $CLEAN_UP_PATH/*`
     find /path/to/base/dir/* -type d -ctime +10 -exec rm -rf {} \;
     if [ $? -eq 0 ]; then
         echo "cleaning is done..." 
     else
        echo "failed to clean the folder..." 
     fi
  fi
fi
