.. table:: Yarn/Mapreduce Logs (`/home/gs/var/log/mapred`)

  +-----------------------------------------------------+----------------------------------------+
  | Log                                                 | Description                            |
  +=====================================================+========================================+
  | yarn-mapred-historyserver-{hostname} |br|           |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from historyserver process|
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-resourcemanager-{hostname} |br|         |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the resource Manager |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-timelineserver-{hostname} |br|          |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the timelineserver   |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-nodemanager-{hostname} |br|             |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the nodemanager      |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-historyserver-{hostname}.out            | Startup of historyserver               |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-resourcemanager-{hostname}.out          | Startup of resourcemanager             |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-timelineserver-{hostname}.out           | Startup of teimlineserver              |
  +-----------------------------------------------------+----------------------------------------+
  | mapred-jobsummary.log(.{yyyy-MM-dd}.bz2)?           | Logs the summary of each completed job |
  +-----------------------------------------------------+----------------------------------------+
  | rm-appsummary.log(.{yyyy-MM-dd}.bz2)?               | the summary of each app                |
  +-----------------------------------------------------+----------------------------------------+
  | rm-audit.log(.{yyyy-MM-dd}.bz2)?                    | List of all operations                 |
  +-----------------------------------------------------+----------------------------------------+
  | rm-auth.log(.{yyyy-MM-dd}.bz2)?                     |  logs the authentications for the RM   |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-jobhistory-jetty.log.{yyyy_MM_dd}(.gz)?      | Runtime logs from historyserver Jetty  |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-resourcemanager-jetty.log.{yyyy_MM_dd}(.gz)? | Runtime logs from the jetty RM         |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-timelineserver-jetty.log.{yyyy_MM_dd}(.gz)?  | Runtime logs from the timeline Jetty   |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-nodemanager-jetty.log.{yyyy_MM_dd}(.gz)?     | Runtime logs from the nodemanager Jetty|
  +-----------------------------------------------------+----------------------------------------+
  | timelineserver-auth.log(.yyy-MM-dd-HH.bz2)?         | Authentications for timelineserver     |
  +-----------------------------------------------------+----------------------------------------+
  | gc-jobhistory.log-{yyyyMMddHHmm}                    | Logs of the GC from jobhistory process |
  +-----------------------------------------------------+----------------------------------------+
  | gc-nodemanager.log-{yyyyMMddHHmm}                   | Logs of the GC from NM process         |
  +-----------------------------------------------------+----------------------------------------+
  | gc-resourcemanager.log-{yyyyMMddHHmm}               | Logs of the GC from RM process         |
  +-----------------------------------------------------+----------------------------------------+
  | gc-timelineserver.log-{yyyyMMddHHmm}                |  GC logs from timelineserver  process  |
  +-----------------------------------------------------+----------------------------------------+