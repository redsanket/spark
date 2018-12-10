package hadooptest.workflow.spark.app;

public enum AppMaster {

    YARN_STANDALONE,
    YARN_CLUSTER,
    YARN_CLIENT;

    public static String getString(AppMaster appmaster) {
        String ret = null;

        if(appmaster == AppMaster.YARN_STANDALONE) {
            ret = "yarn-standalone";
        }
        else if (appmaster == AppMaster.YARN_CLUSTER) {
            ret = "yarn-cluster";
        }
        else if (appmaster == AppMaster.YARN_CLIENT) {
            ret = "yarn-client";
        }

        return ret;
    }
}
