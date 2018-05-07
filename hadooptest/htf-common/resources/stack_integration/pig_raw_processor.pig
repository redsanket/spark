-- stack int test, pig_job1 meant to be run from oozie

set mapred.child.java.opts ' -Djavax.net.ssl.trustStore=/home/gs/conf/current/kms.jks ';

in = load '$INPUTFILE';
out = foreach in generate *;
store out into '$OUTPUTDIR';

