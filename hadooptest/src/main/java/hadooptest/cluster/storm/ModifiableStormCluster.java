package hadooptest.cluster.storm;

public interface ModifiableStormCluster extends StormCluster {
    public void resetConfigsAndRestart() throws Exception;
    public void restartCluster() throws Exception;
    public void setConf(String key, Object value) throws Exception;
    public void unsetConf(String key) throws Exception;
    public void stopCluster() throws Exception;
    public void startCluster() throws Exception;
}
