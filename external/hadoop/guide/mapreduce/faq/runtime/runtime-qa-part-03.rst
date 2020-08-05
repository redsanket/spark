How to separate output into multiple directories
=================================================

Occasionally there is a need to separate the output of a map-reduce job into multiple directories instead of a single directory. These directories represent the type or class of data they contain which is very case specific. For e.x. Assuming that we are processing data using ``TextInputFormat`` from 3 domains - news, sports, finance and would like the final output to be present in the following directory structure -

* ``/my/home/ouput/news/*``,
* ``/my/home/ouput/sports/*``,
* ``/my/home/ouput/finance/*``, following steps should be followed:-


#. Prefix the type name of the (key,value) followed by a separator of your choice (like '\t') to the 'key' in the map or reduce phase.

   .. code-block:: java

      enum Domain { news, sports, finance };
      public void map(WritableComparable lineNumber, Text line,
                      OutputCollector output, Reporter reporter) {
        String inputLine = line.toString();
        String key = readKey(inputLine);
        String value = readValue(inputLine);
        Domain domain = findDomain(key,value);
        key = domain.toString() + "\t" + key;
        ...
        output.collect(new Text(key), new Text(value));
      }

#. Extend `MultipleTextOutputFormat` and override the APIs as follow.
   Note:- The APIs below are called in MultipleOutputFormat in the order as they appear below. The example below is for Text, Text output.


   .. code-block:: java

     class MyMultipleOutputText extends MultipleTextOutputFormat<Text,Text> {
	     /**
	      * Read the key/value prefix information here to generate the path for the output.
	      * @param key
	      *           Input key to the record writer's write() call.
	      * @param v
	      *           Input value to the record writer's write() call.
	      * @param name
	      *           Default output path of the form "part-000*".
	      * @return
	      *           The output file name deduced from key/value
	      */
	     public String generateFileNameForKeyValue(Text key, Text v, String name) {
	       /**
	        * default value for "name" that we receive is of the form "part-000*"
	        * split the default name (for e.x. part-00000 into ['part', '00000'] )
	        */
	       String[] nameparts = name.split("-");
	       String keyStr = key.toString();
	       int typeNameIdx = keyStr.indexOf("\t");
	       // get the file name
	       name = keyStr.substring(0, typeNameIdx);
	       /**
	        * return the path of the form 'fileName/fileName-0000'
	        * This makes sure that fileName dir is created under job's output dir
	        * and all the keys with that prefix go into reducer-specific files under
	        * that dir.
	        */
	       return new Path(name, name + "-" + nameparts[1]).toString();
	     }

	     /**
	      * Strip the type information here to generate the actual key.
	      */
	     public Text generateActualKey(Text key, Text value) {
	       String keyStr = key.toString();
	       int idx = keyStr.indexOf("\t") + 1;
	       return new Text(keyStr.substring(idx));
	     }

	     /**
	      * This API is called in the end in
	      * the MultipleOutputFormat.getRecordWriter().write()
	      * call to get  writer corresponding to the path deduced from the previous APIs.
	      * The writer is cached so any subsequent write() calls resulting in previously
	      * seen path gets the writer from cache.
	      * Any job configuration specific initialization should be done here.
	      * @param fs
	      *          the file system to use
	      * @param job
	      *          a job conf object
	      * @param name
	      *          the name of the file over which a record writer object will be
	      *          constructed
	      * @param reporter
	      *          a progressable object
	      * @return A RecordWriter object over the given file
	      * @throws IOException
	      */
	      public RecordWriter<Text,Text> getBaseRecordWriter(FileSystem fs, JobConf job,
	        String name, Progressable reporter) throws IOException {
	        // Initialization code here
	        super.getBaseRecordWriter(fs, job, name, reporter);
	      }
	    }



#. Set the output format in jobConf

   .. code-block:: java

      jobConf.setOutputFormat(MyMultipleOutputText.class);


What is the maximum value one can use for ``mapreduce.task.timeout``?
=====================================================================

Maximum would be 0. That would disable the timeout.


How to find out what input file your mapper task is operating on
=================================================================

Look at the task's environment variable ``mapreduce.map.input.file``. Some other useful environment variables are ``mapreduce.map.input.length``, ``mapreduce.map.input.start`` and ``mapreduce.task.id`` (the first identifies the attempt, the second the task being attempted.
More are described in: Configured Parameters in the MapReduce documentation.

For a full list, try running:

  .. code-block:: bash

    hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
           -Dmapreduce.job.queuename=unfunded -input x-in -output x_out \
           -mapper 'sh -c "printenv"' -reducer cat


where ``x_in`` is any dummy input file in HDFS and ``x_out/part-00000`` will contain the results from the ``printenv`` command.

How to access information on finished jobs?
===========================================

Job History API provides users to get status and logs of finished applications in raw format.
Both of the following URI’s give you the history server information, from an application id identified by the appid value following HTTP/GET request.


  .. code-block:: bash

     http://<ip-address:port>/ws/v1/history
     http://<ip-address:port>/ws/v1/history/info

This request has to be made with a valid YBY cookie of a user who has view-acls enabled for this job. The history contents are returned in raw format (which is ``JSON`` format) and hence should allow automation tools to be able to process the history files.

The jobs resource provides a list of the MapReduce jobs that have finished. It does not currently return a full list of parameters

When you make a request for the list of jobs, the information will be returned as an array of job objects. See also Job API for syntax of the job object. Except this is a subset of a full job. Only ``startTime``, ``finishTime``, ``id``, ``name``, ``queue``, ``user``, ``state``, ``mapsTotal``, ``mapsCompleted``, ``reducesTotal``, and ``reducesCompleted`` are returned.


  .. code-block:: bash

     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/jobattempts
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/counters
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/conf
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}
     GET http://<ip-address:port>/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters

For more details on the response, see :hadoop_rel_doc:`MapReduce History Server REST API’s <hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html>`.

:guilabel:`logs`

Job history files are also logged to user specified directory ``mapreduce.jobhistory.intermediate-done-dir`` and ``mapreduce.jobhistory.done-dir``, which defaults to job output directory.

User can view the history logs summary in specified directory using the following command 

  .. code-block:: bash

     mapred job -history output.jhist

The above command will print job details, failed and killed tip details. More details about the job such as successful tasks and task attempts made for each task can be viewed using the following command

  .. code-block:: bash

     mapred job -history all output.jhist

Normally the user uses Job to create the application, describe various facets of the job, submit the job, and monitor its progress.


.. important:: It is hightly discouraged to have applications doing screen scraping of web-ui for status of queues/jobs or worse, job-history of completed jobs.
               With Job History API, you should be able read history files from an Automated process without putting load on ``ResourceManager``.
