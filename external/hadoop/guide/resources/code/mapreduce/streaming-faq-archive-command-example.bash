hadoop jar hadoop-streaming.jar \
    -archives 'hdfs://hadoop-nn1.domain.com/user/me/samples/cachefile/cachedir.jar' \
    -D mapreduce.job.maps=1 \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.name="Experiment" \
    -input "/user/me/samples/cachefile/input.txt" \
    -output "/user/me/samples/cachefile/out" \
    -mapper "xargs cat" \
    -reducer "cat"

$ ls test_jar/
cache.txt  cache2.txt

$ jar cvf cachedir.jar -C test_jar/ .
added manifest
adding: cache.txt(in = 30) (out= 29)(deflated 3%)
adding: cache2.txt(in = 37) (out= 35)(deflated 5%)

$ hdfs dfs -put cachedir.jar samples/cachefile

$ hdfs dfs -cat /user/me/samples/cachefile/input.txt
cachedir.jar/cache.txt
cachedir.jar/cache2.txt

$ cat test_jar/cache.txt
This is just the cache string

$ cat test_jar/cache2.txt
This is just the second cache string

$ hdfs dfs -ls /user/me/samples/cachefile/out
Found 2 items
-rw-r--r-* 1 me supergroup   0 2013-11-14 17:00 /user/me/samples/cachefile/out/_SUCCESS
-rw-r--r-* 1 me supergroup   69 2013-11-14 17:00 /user/me/samples/cachefile/out/part-00000

$ hdfs dfs -cat /user/me/samples/cachefile/out/part-00000
This is just the cache string
This is just the second cache string