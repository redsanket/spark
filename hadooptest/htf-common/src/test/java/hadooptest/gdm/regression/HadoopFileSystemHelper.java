// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression;

import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.gdm.ConsoleHandle;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HadoopFileSystemHelper implements PrivilegedExceptionAction<String> {
    
    public enum CommandEnum {
        CreateDir, CreateFile, FileExists, NumFiles, GetFileStatus
    }
    
    private Configuration configuration;
    private String fullPath;
    private UserGroupInformation ugi;
    private CommandEnum command = CommandEnum.CreateDir;
    private FileStatus fileStatus;
    private String crcValue;
    
    
    /**
     * Object used to manipulate the filesystem of a Hadoop grid for GDM tests
     * 
     * @param  gridName  name of Hadoop grid
     * @throws IOException  
     */
    public HadoopFileSystemHelper(String gridName) throws IOException {
        this.configuration = this.getConfiguration(gridName);
        this.ugi = getUGI();
        ConsoleHandle consoleHandle = new ConsoleHandle();
        org.apache.commons.configuration.Configuration configuration = consoleHandle.getConf();
        this.crcValue = configuration.getString("hostconfig.console.crcValue").trim();
    }
    
    /**
     * Creates a directory on the grid
     * 
     * @param  directoryPath  
     * @throws IOException, InterruptedException
     */
    public void createDirectory(String directoryPath) throws IOException, InterruptedException {
        this.fullPath = directoryPath;
        this.command = CommandEnum.CreateDir;
        this.ugi.doAs(this);
    }
    
    /**
     * Creates a file on the grid
     * 
     * @param  fullPath  
     * @throws IOException, InterruptedException
     */
    public void createFile(String fullPath) throws IOException, InterruptedException {
        this.fullPath = fullPath;
        this.command = CommandEnum.CreateFile;
        this.ugi.doAs(this);
    }
    
    /**
     * Checks if a file exists on the grid
     * 
     * @param  fullPath  
     * @return true if the path exists, false otherwise
     * @throws IOException, InterruptedException
     */
    public boolean exists(String fullPath) throws IOException, InterruptedException {
        this.fullPath = fullPath;
        this.command = CommandEnum.FileExists;
        String exists = this.ugi.doAs(this);
        if (exists.equalsIgnoreCase("TRUE")) {
            return true;
        } else {
            return false;
        }
    }
    
    public FileStatus getFileStatus(String fullPath) throws IOException, InterruptedException {
        this.fullPath = fullPath;
        this.command = CommandEnum.GetFileStatus;
        this.ugi.doAs(this);
        return this.fileStatus;
    }
    
    /**
     * Gets the number of files for a specified path
     * 
     * @param  fullPath  
     * @return the number of files at the path
     * @throws IOException, InterruptedException
     */
    public int numFiles(String fullPath) throws IOException, InterruptedException {
        this.fullPath = fullPath;
        this.command = CommandEnum.NumFiles;
        String result = this.ugi.doAs(this);
        return Integer.parseInt(result);
    }
    
    private UserGroupInformation getUGI() throws IOException {
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(HadooptestConstants.UserNames.DFSLOAD + "@DEV.YGRID.YAHOO.COM", 
                HadooptestConstants.Location.Keytab.DFSLOAD);
    }
    
    /**
     * Runs a UGI command
     * 
     * @return the result of the command
     * @throws Exception
     */
    public String run() throws Exception {
        String result = "";
        switch (this.command) {
        case CreateDir:
            runCreateDirCommand();
            break;
        case CreateFile:
            runCreateFileCommand();
            break;
        case FileExists:
            result = runFileExistsCommand();
            break;
        case NumFiles:
            result = runNumFilesCommand();
            break;
        case GetFileStatus:
            runGetFileStatus();
            break;
        default:
            throw new IOException("Unsupported operation - " + this.command);
        }
        
        return result;
    }
    
    private String runNumFilesCommand() throws IOException {
        FileSystem fs = FileSystem.get(this.configuration);
        Path path = new Path(this.fullPath);
        FileStatus[] status = fs.globStatus(new Path(this.fullPath));
        return Integer.toString(status.length);
    }
    
    private String runFileExistsCommand() throws IOException {
        FileSystem fs = FileSystem.get(this.configuration);
        Path path = new Path(this.fullPath);
        if (fs.exists(path)) {
            return "TRUE";
        } else {
            return "FALSE";
        }
    }
    
    private void runCreateDirCommand() throws IOException {
        FileSystem fs = FileSystem.get(this.configuration);
        Path path = new Path(this.fullPath);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }
    
    private void runCreateFileCommand() throws IOException {
        // create the base directory
        File f = new File(this.fullPath);
        Path path = new Path(f.getParent());
        FileSystem fs = FileSystem.get(this.configuration);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        
        // create the file
        path = new Path(this.fullPath);
        FSDataOutputStream fsDataOutPutStream = fs.create(path);

        // write some data
        int len = 100;
        byte[] data = new byte[len];
        for (int k = 0; k < len; k++) {
            data[k] = new Integer(k).byteValue();
        }
        fsDataOutPutStream.write(data);
        fsDataOutPutStream.close(); 
    }
    
    private void runGetFileStatus() throws IOException {
        FileSystem fs = FileSystem.get(this.configuration);
        Path path = new Path(this.fullPath);
        this.fileStatus = fs.getFileStatus(path);
    }
    
    private Configuration getConfiguration(String gridName) {
        String nameNode = new ConsoleHandle().getClusterNameNodeName(gridName);
        Configuration conf = new Configuration(true);
        String defaultFs = "hdfs://" + nameNode + ":" + HadooptestConstants.Ports.HDFS;
        conf.set("fs.defaultFS", defaultFs);
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@DEV.YGRID.YAHOO.COM");
        conf.set("hadoop.security.authentication", "true");
        if (this.crcValue != null) {
            conf.set("dfs.checksum.type" , this.crcValue);
        } else {
            conf.set("dfs.checksum.type" , "CRC32");
        }
        return conf;
    }
    
}


