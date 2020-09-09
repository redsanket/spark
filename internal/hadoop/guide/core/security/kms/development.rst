***********
Development
***********


Testing
=======

.. _security_kms_development_qe_testing:

KMS QE Cluster Development Testing
----------------------------------

Last Modified Date: Augest 28, 2020

Steps:
------
0. Make sure KMS is deployed from y-branch-2.8 into QE clusters, or the deployment may cause conflicting dependency issues.
1. In the node that runs namenode, find which node runs KMS by searching ``hadoop.security.key.provider.uris`` in ``/home/gs/conf/current/hdfs-server-site.xml``.
2. In the node that runs KMS, run ``yinst ls yahoo_kms -files`` to find where KMS is installed in the cluster. In openqe178blue, configs are placed in ``/home/y/conf/kms``, jars (like kms.war) are placed in ``/home/y/libexec/webapps``, and logs are stored in ``/home/y/logs/yjava_jetty``, which includes:

    * server.log – Output from yjava_jetty.  May contain information useful to debug why the service did not start, or if jetty is malfunctioning.
    * yjava_jetty.out – Output from yjava_jetty.  Nothing very useful is in this log. 
    * access.log – Contains all http requests.  Most clients other than the RMs should be using the RPC service but older or non-yahoo hadoop releases will use the REST service.  Useful to root cause abusive users.
    * kms.log – Contains startup information for the kms servlet, all RPC authentications, and all token related operations.  Useful to root cause abusive users, ZK connectivity issues, and token operations.
    * kms-audit.log – Contains aggregated per user kms operation counts over a 10s interval.  Also contains noisy REST “OPTIONS” calls.  Useful to determine if the kms is servicing requests or if a user is being abusive.  The operations should be exclusively DECRYPT_EEK for users, and GENERATE_EEK for hdfs namenodes.
    * gc.log – Standard java gc logging.  Useful to determine if the kms is under excessive memory pressure or in full GC.  The heap is small enough that full GC should take a second.

3. Use ``mvn clean -fn install package -Pdist,native -DskipTests -Dmaven.javadoc.skip=true`` to build ``kms.war`` in ``hadoop-common-project/hadoop-kms``. If the native build (C/C++ code compilation) fails, just use -Pdist.
4. KMS runs as a webapp under Jetty server, and its runtime tries to isolate the app from the server environment using a separate class loader. Therefore, 

    * The webapp class loader will search classes in the war first and then fall back to the jvm's classpath. If you make changes in other directories (such as ``hadoop-common-project/hadoop-common``), make sure you build jars there before building ``kms.war``.
    * KMS's classpath is different from other hadoop services. If a file is not on the classpath, you can try to add the path to "extraClasspath" directive in ``/home/y/libexec/webapps/kms.xml``.

5. Run ``yinst restart yahoo_kms`` to restart KMS service to load changes.





