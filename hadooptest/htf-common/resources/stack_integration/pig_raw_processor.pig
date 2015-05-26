-- stack int test, pig_job1 meant to be run from oozie

in = load '$INPUTFILE';
out = foreach in generate *;
store out into '$OUTPUTDIR';

