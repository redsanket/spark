bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

HADOOP_QA_HOME=$bin/..


dryrun=${1:-true}
for test in \
tests/Regression/YARN/JobSummaryInfo/run_JobSummaryInfo.sh \
tests/Benchmarks/YARN/TeraSort/run_TeraSort.sh \
tests/Benchmarks/YARN/DFSIO/run_DFSIO.sh \
tests/Benchmarks/YARN/Sort/run_Sort.sh \
tests/Benchmarks/YARN/Shuffle/run_Shuffle.sh \
tests/Benchmarks/YARN/SmallJobs/run_SmallJobs.sh \
tests/Benchmarks/YARN/Scan/run_Scan.sh \
tests/EndToEnd/YARN/Streaming/run_StreamingTests.sh \
tests/EndToEnd/YARN/Compression/run_Compression.sh \
tests/EndToEnd/YARN/Pipes/run_Pipes.sh
do
  log_file=$HADOOP_QA_HOME/`echo $test | sed 's/\// /g' | awk '{print $NF}' | sed 's/sh/log/g' | sed 's/run_//g'`
	cmd="${HADOOP_QA_HOME}/bin/driver.sh -c omegab -w $HADOOP_QA_HOME -n -s $HADOOP_QA_HOME/$test 2>&1 | tee $log_file | less"
  echo "$cmd"
	if [ $dryrun == false ]; then
		$cmd
		echo "$test Exited with response code: $?" | tee -a $log_file
	fi	
done
