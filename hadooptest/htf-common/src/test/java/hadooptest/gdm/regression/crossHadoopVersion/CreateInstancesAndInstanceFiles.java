// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.crossHadoopVersion;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

public class CreateInstancesAndInstanceFiles implements PrivilegedExceptionAction<String> {

    private String clusterName;
    private String basePath;
    private String dataPath;
    private String nameNodeName;
    private ConsoleHandle consoleHandle;
    private Configuration configuration;
    private List<String> instanceList;
    private static final String schema = HadooptestConstants.Schema.HDFS;
    private static final String PROCOTOL = "hdfs://";
    private static final String KEYTAB_DIR = "keytabDir";
    private static final String KEYTAB_USER = "keytabUser";
    private static final String OWNED_FILE_WITH_COMPLETE_PATH = "ownedFile";
    private static final String USER_WHO_DOESNT_HAVE_PERMISSIONS = "userWhoDoesntHavePermissions";
    private static final int NO_OF_INSTANCE = 5;
    private static final int DAYS = 20;
    private static HashMap<String, HashMap<String, String>> supportingData = new HashMap<String, HashMap<String, String>>();

    public CreateInstancesAndInstanceFiles(String clusterName , String basePath , String dataPath ) {

        this.clusterName = clusterName;
        this.basePath = basePath;
        this.dataPath = dataPath;

        // Populate the details for DFSLOAD
        HashMap<String, String> fileOwnerUserDetails = new HashMap<String, String>();
        fileOwnerUserDetails = new HashMap<String, String>();
        fileOwnerUserDetails.put(KEYTAB_DIR, HadooptestConstants.Location.Keytab.DFSLOAD);
        fileOwnerUserDetails.put(KEYTAB_USER, HadooptestConstants.UserNames.DFSLOAD + "@DEV.YGRID.YAHOO.COM");
        fileOwnerUserDetails.put(OWNED_FILE_WITH_COMPLETE_PATH, "/tmp/"+ HadooptestConstants.UserNames.DFSLOAD + "Dir/" + HadooptestConstants.UserNames.DFSLOAD + "File");
        fileOwnerUserDetails.put(USER_WHO_DOESNT_HAVE_PERMISSIONS, HadooptestConstants.UserNames.HADOOPQA);

        this.supportingData.put(HadooptestConstants.UserNames.DFSLOAD,fileOwnerUserDetails);
        TestSession.logger.info("CHECK:" + this.supportingData);
        this.consoleHandle = new ConsoleHandle();
        this.nameNodeName = this.consoleHandle.getClusterNameNodeName(this.clusterName);

        this.instanceList = new ArrayList<String>();

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        TestSession.logger.info(sdf.format(cal.getTime()));

        cal.add(Calendar.DAY_OF_MONTH, -DAYS);
        String instance = sdf.format(cal.getTime());
        this.instanceList.add(instance);

        for (int i = 1 ; i < NO_OF_INSTANCE  ; i++) {
            cal.add(Calendar.DAY_OF_MONTH, i);
            instance = sdf.format(cal.getTime());
            this.instanceList.add(instance);
        }
    }

    /*
     * Return instance List
     */
    public List<String> getInstanceList() {
        return this.instanceList;
    }

    /*
     * Invoke run method and create the instance files on the specified cluster.
     */
    public void execute() throws IOException, InterruptedException {
        for (String aUser : this.supportingData.keySet()) {
            TestSession.logger.info("aUser = " + aUser);
            this.configuration = getConfForRemoteFS();
            UserGroupInformation ugi = getUgiForUser(aUser);

            String result = ugi.doAs(this);
            TestSession.logger.info("Result = " + result);
        }
    }

    /**
     * Returns the remote cluster configuration object.
     * @param aUser  - user
     * @param nameNode - name of the cluster namenode. 
     * @return
     */
    public Configuration getConfForRemoteFS() {
        Configuration conf = new Configuration(true);
        String namenodeWithChangedSchemaAndPort = this.PROCOTOL + this.nameNodeName + ":" + HadooptestConstants.Ports.HDFS;
        TestSession.logger.info("For HDFS set the namenode to:[" + namenodeWithChangedSchemaAndPort + "]");
        conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@DEV.YGRID.YAHOO.COM");
        conf.set("hadoop.security.authentication", "true");
        conf.set("dfs.checksum.type" , "CRC32C");
        TestSession.logger.info(conf);
        return conf;
    }

    /**
     * set the hadoop user details , this is a helper method in creating the configuration object.
     */
    public UserGroupInformation getUgiForUser(String aUser) {
        String keytabUser = this.supportingData.get(aUser).get(KEYTAB_USER);
        TestSession.logger.info("Set keytab user=" + keytabUser);
        String keytabDir = this.supportingData.get(aUser).get(KEYTAB_DIR);
        TestSession.logger.info("Set keytab dir=" + keytabDir);
        UserGroupInformation ugi = null;
        try {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabUser, keytabDir);
            TestSession.logger.info("UGI=" + ugi.toString());
            TestSession.logger.info("credentials:" + ugi.getCredentials());
            TestSession.logger.info("group names" + ugi.getGroupNames());
            TestSession.logger.info("real user:" + ugi.getRealUser());
            TestSession.logger.info("short user name:" + ugi.getShortUserName());
            TestSession.logger.info("token identifiers:" + ugi.getTokenIdentifiers());
            TestSession.logger.info("tokens:" + ugi.getTokens());
            TestSession.logger.info("username:" + ugi.getUserName());
            TestSession.logger.info("current user:" + UserGroupInformation.getCurrentUser());
            TestSession.logger.info("login user:" + UserGroupInformation.getLoginUser());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ugi;
    }

    public String run() throws Exception {
        String returnValue = "false";
        TestSession.logger.info("configuration   =  " + this.configuration.toString());
        FileSystem remoteFS = FileSystem.get(this.configuration);
        Path path = new Path(this.basePath.trim());

        // check whether remote path exists on the grid
        boolean basePathExists = remoteFS.exists(path);
        if(basePathExists == false) {
            TestSession.logger.info( this.basePath + "  does not exists, creating one.");
            FsPermission fsPermission = new FsPermission(FsAction.ALL , FsAction.ALL , FsAction.ALL);
            boolean basePathCreated = remoteFS.mkdirs(path,  fsPermission);
            if (basePathCreated == true) {
                TestSession.logger.info(this.basePath + " successfully created.");
                createTestDirectory(remoteFS , path);
                basePathExists = true;
            } else {
                TestSession.logger.info("Failed to create " + this.basePath + " directories.");
            }
        }  if (basePathExists == true) {
            createTestDirectory(remoteFS , path);

            TestSession.logger.info("Following are the instances");
            for ( String instance : this.instanceList ) {
                TestSession.logger.info(instance);

                Path instancePath = new Path( this.basePath +  "/" + this.dataPath + "/" + instance );
                boolean isInstanceCreated =  remoteFS.mkdirs(instancePath);
                TestSession.logger.info( instancePath.toString() + " is ceated " + isInstanceCreated );
                assertTrue("Failed to create instance directory - " + this.basePath + "/" + this.dataPath + "/" + instance , isInstanceCreated == true);

                String destFile = this.basePath +  "/" + this.dataPath + "/" + instance + "/" + "instanceFile" + ".gz";
                TestSession.logger.info("destFile  = " + destFile);

                Path destFilePath = new Path(destFile);
                FSDataOutputStream fsDataOutPutStream = remoteFS.create(destFilePath, false);

                // create a byte array of 350MB (367001600) bytes
                int len = 36700;
                byte[] data = new byte[len];
                for (int k = 0; k < len; k++) {
                    data[k] = new Integer(k).byteValue();
                }
                fsDataOutPutStream.write(data);
                fsDataOutPutStream.close();
                TestSession.logger.info( destFile  + " succcessfully created.");
                returnValue = "success";
            }
        }
        return returnValue;
    }

    public void createTestDirectory(FileSystem remoteFS , Path path) throws IOException {
        FileStatus fileStatus = remoteFS.getFileStatus(path);
        FsPermission fsPermission = fileStatus.getPermission();
        TestSession.logger.info(this.clusterName + " " + this.basePath +  " is " + fsPermission.toString());
        String permission = fsPermission.toString();

        // check and change permission if permission is not equal to 777 
        if (! permission.equals("rwxrwxrwx")) {
            remoteFS.setPermission(path, FsPermission.createImmutable((short) 0777));
            TestSession.logger.info(path.toString() + " changed to permission  0777 on " + this.clusterName);    
        }
        String destFolder = this.basePath.trim() + "/" + this.dataPath;
        Path path1 = new Path(destFolder);
        boolean dirFlag = remoteFS.mkdirs(path1);
        assertTrue("Failed to create " +  destFolder  , dirFlag == true);
        TestSession.logger.info(destFolder + " created Sucessfully.");
    }


}
