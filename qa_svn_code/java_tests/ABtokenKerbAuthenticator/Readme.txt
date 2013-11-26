20.205 version of wordcount.java, modified to exhibit the token renewal issue
seen on AxoniteBlue per 4994229/hadoop7853, phw 20120304 

The jar file in this location should work as-is as long as .23 release does not have
a binary compatibility issue in the APIs used, which it should not.

Usage is very straight-forward, the job is wordcount only it passes an invalid
token, this should be ignored and reauthentication occurs sucessfully by a re-login.
In the fail case, the relogin attempt fails because an incorrect authentication
is used after 80% of the Kerb ticket expiration time (default is 24 hours, so
fails after 24x0.80 hours).

To run:

1. deploy release to test cluster
2. allow cluster to remain up for 24 hours, to ensure the Kerb expiration occurs
3. fs -copyFromLocal the 'test_file.txt' to your cluster, for wordcount's input
4. create an output path on your cluster, for the wordcount to use
5. run the job:
	<HadoopPath>/hadoop --config <ConfigPath>  jar <JarPath>/wordcount23dot2token.jar  \
             wordcountABtoken23dot2 <InputPath> <OutputPath> 

To build:

1. In my case, i just compiled using the release's classpath, but set your
   classpath as desired to compile hadoop

2. compile the modified wordcount code;
     mkdir out
     javac -Xlint:deprecation  -cp  /home/gs/gridre/yroot.omegak/share/hadoop/*:\
       /home/gs/gridre/yroot.omegak/share/hadoop/lib/*:/home/gs/gridre/yroot.omegak/share/hadoop/modules/*\
       -d out wordcountABtoken23dot2.java

   Note: you get 1 warning because getDelegationToken is deprecated:
      wordcountABtoken23dot2.java:77: warning: [deprecation]
      getDelegationToken(java.lang.String) in org.apache.hadoop.fs.FileSystem has
      been deprecated
           Token<?> token = fs.getDelegationToken("NotGoodToken");
                        ^
      1 warning

3. Jar up the class files;
     jar -cvf  wordcount23dot2token.jar  -C out .

4. result should be the same jar as is checked in, use this jar as noted in "To run"

