package hadooptest.cluster.storm;

public abstract class ModifiableStormCluster extends StormCluster {
    public abstract void resetConfigsAndRestart() throws Exception;
    public abstract void restartCluster() throws Exception;
    public abstract void setConf(String key, Object value) throws Exception;
    public abstract void unsetConf(String key) throws Exception;
    public abstract void stopCluster() throws Exception;
    public abstract void startCluster() throws Exception;
    public abstract void stopRegistryServer() throws Exception;
    public abstract void startRegistryServer() throws Exception;
    public abstract void setRegistryServerURI(String uri) throws Exception;
    public abstract void unsetRegistryConf(String key) throws Exception;
    public abstract void setRegistryConf(String key, Object value) throws Exception;
    public abstract String getFromRegistryServer(String endpoint) throws Exception ;
    public abstract boolean isVirtualHostDefined(String vhName);
    public abstract String getBouncerUser() throws Exception;
    public abstract String getBouncerPassword() throws Exception;
}
