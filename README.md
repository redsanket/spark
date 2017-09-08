Hadoop Cluster Deployment
==================

## Jenkins Job Parameters
<br>

### Core Hadoop

| Param Name                    | REQ   | Param Type | Default Value                    | Comment                                                   |
| :---:                         | :---: | :---:      | :---:                            | --------------------------------------------------------- |
| CLUSTER                       | Y     | string     |                                  |                                                           |
| HADOOP_RELEASE_TAG            | Y     | string     |                                  | *HADOOP_2_8_0_LATEST*                                     |
| REMOVEEXISTINGDATA            | N     | boolean    | true                             | If true will WIPE OUT ALL DATA from your HDFS data store. |
<br>

### Stack Components

#### Stack Components - Tez
| Param Name                    | REQ   | Param Type | Default Value                    | Comment                                                   |
| :---:                         | :---: | :---:      | :---:                            | --------------------------------------------------------- |
| INSTALL_TEZ                   | N     | string     | false                            | Value of 'only' will just install Tez and exit.           |
| TEZ_DIST_TAG                  | Y*    | string     |                                  | Required if Tez is enabled. *TEZ_DOT_NINE*                |
| TEZVERSION                    | N     | string     | 'none'                           | This will be ignored if TEZ_DIST_TAG is defined           |
| TEZ_QUEUE                     | N     | string     | default                          |                                                           |
<br>

#### Stack Components - Spark
| Param Name                    | REQ   | Param Type | Default Value                    | Comment                                                   |
| :---:                         | :---: | :---:      | :---:                            | --------------------------------------------------------- |
| STACK_COMP_INSTALL_SPARK      | N     | boolean    | false                            |                                                           |
| SPARK_DIST_TAG                | Y*    | string     |                                  | *yspark_yarn_1_6_certified* Determine SPARKVERSION        |
| SPARKVERSION                  | N     | string     | 'none'                           | This will be ignored if SPARK_DIST_TAG is defined         |
| STACK_COMP_VERSION_SPARK      | N     | string     | 'none'                           | To determine SPARK_SHUFFLE_VERSION . Install Spark component on the Spark node using a reference cluster's package version(s), none - do not install, LATEST - use version from Artifactory LATEST, axonitered - use same version as on AR cluster. |
| SPARK_SHUFFLE_DIST_TAG        | N     | string     | See comment                      | Default value is 'same_as_STACK_COMP_VERSION_SPARK'. To determine SPARK_SHUFFLE_VERSION |
| SPARK_HISTORY_SERVER_DIST_TAG | Y*    | string     |                                  | *yspark_yarn_history_server_1_6_certified* Determine SPARK_HISTORY_VERSION |
| SPARK_QUEUE                   | N     | string     | default                          | default,grideng                                           |
<br>

#### Stack Components - Misc.
| Param Name                    | REQ   | Param Type | Default Value                    | Comment                                                   |
| :---:                         | :---: | :---:      | :---:                            | --------------------------------------------------------- |
| STACK_COMP_VERSION_HIVE       | N     | string     | 'none'                           | {none,LATEST,axonitered,current} Install Hive component on the Hive node using a reference cluster's package version(s), none - do not install, LATEST - use version from Artifactory LATEST, axonitered - use same version as on AR cluster, current - use version on 'current' Dist branch |
| STACK_COMP_VERSION_OOZIE      | N     | string     | 'none'                           | {none,LATEST,axonitered} Install Oozie component on the Oozie node using a reference cluster's package version(s), none - do not install, LATEST - use version from Artifactory LATEST, axonitered - use same version as on AR cluster |
| STACK_COMP_VERSION_PIG        | N     | string     | 'none'                           | {none,LATEST,axonitered} Install Pig component on the Gateway node using a reference cluster's package version(s), none - do not install, LATEST - use version from Artifactory LATEST, axonitered - use same version as on AR cluster |
| HBASE_SHORTCIRCUIT            | N     | string     | false                            |                                                           |
<br>

### Not Frequently Used

| Param Name                    | REQ   | Param Type | Default Value                    | Comment                                                   |
| :---:                         | :---: | :---:      | :---:                            | --------------------------------------------------------- |
| HOMEDIR                       | N     | string     | /home                            | /home or /homes                                           |
| USE_DEFAULT_QUEUE_CONFIG      | N     | boolean    | true                             | If selected, this option will cause the capacity scheduler to configure the the 'default' and 'grideng' subqueues. If unselected, this option will cause the capacity scheduler to configure the queues as defined by the local queue configs on the Resource Manager's local filesystem. |
| AUTO_CREATE_RELEASE_TAG       | N     | string     | 0                                | {0,1} Value of 1 will enable auto create dist_tag         |
| YJAVA_JDK_VERSION             | N     | string     | qedefault                        |                                                           |
| ADMIN_HOST                    | N     | string     | devadm102.blue.ygrid.yahoo.com   |                                                           |
| ENABLE_HA                     | N     | string     | false                            |                                                           |
| QA_PACKAGES                   | N     | string     | "hadoop_qe_runasroot-stable ..." | hadoop_qe_runasroot-stable datanode-test hadoop_qa_restart_config-test namenode-test secondarynamenode-test resourcemanager-test nodemanager-test historyserver-test |
| START_STEP                    | N     | string     | 0                                |  For debugging                                            |
| INSTALL_GW_IN_YROOT           | N     | boolean    | false                            |                                                           |
<br>
