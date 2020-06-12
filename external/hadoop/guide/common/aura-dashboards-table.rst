.. table:: `Grid - AR - Aura Dashboard Links Doc sheet <https://docs.google.com/spreadsheets/d/151PGU8Z3Pryrql-sVv1Mw1rioHuM0VYSJvkwza47xBQ>`_ as June 10th 2020.
  :widths: auto

  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |    Component    |                                                   URL                                                   |               Checks               |                   Notes                   |
  +=================+=========================================================================================================+====================================+===========================================+
  |                 |                                                                                                         | HDFS-Missing_Blocks |br|           |                                           |
  |                 |                                                                                                         | NameNode-RPC-Connection-Stats |br| |                                           |
  |                 | `YGrid-AR-NameNode <https://aura.yamas.ouroath.com/#/aura/nducnsfzta/YGrid-AR-NameNode>`_               | check_hdfs |br|                    |                                           |
  |     NameNode    |                                                                                                         | check_hdfs_fullnodes |br|          |                                           |
  |                 |                                                                                                         | check_hdfs_state |br|              |                                           |
  |                 |                                                                                                         | check_namedir |br|                 |                                           |
  |                 |                                                                                                         | proc_namenode                      |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |                 |                                                                                                         | historyserver_health |br|          |                                           |
  |                 |                                                                                                         | proc_historyserver |br|            |                                           |
  |                 |                                                                                                         | proc_resourcemanager |br|          |                                           |
  | ResourceManager | `YGrid-AR-ResourceManager <https://aura.yamas.ouroath.com/#/aura/gogawlngam/YGrid-AR-ResourceManager>`_ | proc_sparkhistoryserver |br|       |                                           |
  |                 |                                                                                                         | proc_timelineserver |br|           |                                           |
  |                 |                                                                                                         | resourcemanager_health |br|        |                                           |
  |                 |                                                                                                         | yarn_capacity |br|                 |                                           |
  |                 |                                                                                                         | yarn_health                        |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |    HDFSProxy    | `YGrid-AR-HDFSProxy <https://aura.yamas.ouroath.com/#/aura/zpeatvopvr/YGrid-AR-HDFSProxy>`_             | hdfspxy_metrics                    |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |   HiveServer2   | `YGrid-AR-HiveServer2 <https://aura.yamas.ouroath.com/#/aura/rpsapghdse/YGrid-AR-HiveServer2>`_         | hs2_rotation |br|                  |                                           |
  |                 |                                                                                                         | proc_hiveserver2                   |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |     HCatalog    | `YGrid-AR-HCatalog <https://aura.yamas.ouroath.com/#/aura/tkfyhuxxwf/YGrid-AR-HCatalog>`_               | proc_hcatserver                    |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |                 |                                                                                                         |                                    | VIP check pending - the ygrid corp |br|   |
  |                 |                                                                                                         |                                    | snode could not connect to |br|           |
  |                 |                                                                                                         | hue_livy |br|                      | api endpoint vipviewer |br|               |
  |       Hue       | `YGrid-AR-Hue <https://aura.yamas.ouroath.com/#/aura/cbexesjsyj/YGrid-AR-Hue>`_                         | hue_ui                             | Eric talked to Rick missing httpfs |br|   |
  |                 |                                                                                                         |                                    | check,                                    |
  |                 |                                                                                                         |                                    | need to include yms_check_httpfs |br|     |
  |                 |                                                                                                         |                                    | indeployment pipeline                     |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |                 |                                                                                                         | jupyter_livy |br|                  | VIP check pending - the ygrid corp |br|   |
  |     Jupyter     | `YGrid-AR-Jupyter <https://aura.yamas.ouroath.com/#/aura/ogeombeicd/YGrid-AR-Jupyter>`_                 | jupyter_ui                         | snode could not connect to vipviewer |br| |
  |                 |                                                                                                         |                                    | api endpoint                              |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |      Oozie      | `YGrid-AR-Oozie <https://aura.yamas.ouroath.com/#/aura/hjlezzcmzi/YGrid-AR-Oozie>`_                     | oozie_rotation |br|                |                                           |
  |                 |                                                                                                         | proc_oozie_jetty                   |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
  |       KMS       | `YGrid-AR-KMS <https://aura.yamas.ouroath.com/#/aura/clnkzekryk/YGrid-AR-KMS>`_                         | kms_auth_check |br|                |                                           |
  |                 |                                                                                                         | kms_vips                           |                                           |
  +-----------------+---------------------------------------------------------------------------------------------------------+------------------------------------+-------------------------------------------+
