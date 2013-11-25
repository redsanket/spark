package hadooptest.cluster.gdm;

import coretest.Util;
import hadooptest.TestSession;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

public class FeedGenerator {
    private String feedName = null;
    
    /**
     * Allows generating a generic feed with a desired name within a one node fdiserver
     *
     * @param feedName  the desired feed name
     */
    public FeedGenerator(String feedName) {
        this.feedName = feedName;
    }
    
    /**
     * Generates an instance of the generic feed for the given instance date
     *
     * @param instance  the desired instance date string
     */
    public void generateFeed(String instance) throws IOException {
        String tmpFeedPath = "/tmp/" + this.feedName + "/" + instance + "/DONE";
        File tmpFeedDir = new File(tmpFeedPath);
        FileUtils.deleteDirectory(tmpFeedDir);
        FileUtils.forceMkdir(tmpFeedDir);
        
        tmpFeedPath = tmpFeedPath + "/";
        String sourceFeedPath = Util.getResourceFullPath("gdm/feeds/generic") + "/";
        
        // setup schema
        {
            File srcSchemaFile = new File(sourceFeedPath + "schema.dat");
            File dstSchemaFile = new File(tmpFeedPath + "HEADER." + this.feedName + "." + instance);
            FileUtils.copyFile(srcSchemaFile, dstSchemaFile);
        }
        
        // setup row count - data has 26 rows
        {
            File dstRowCountFile = new File(tmpFeedPath + "ROWCOUNT." + this.feedName + "." + instance);
            String rowCountData = "26 /tmp/FDIROOT/" + this.feedName + "/" + instance + "/DONE/" + this.feedName + "_0";
            FileUtils.writeStringToFile(dstRowCountFile, rowCountData);
        }
        
        // setup status
        {
            File dstStatusFile = new File(tmpFeedPath + "STATUS." + this.feedName + "." + instance);
            String statusData = "/tmp/FDIROOT/" + this.feedName + "/" + instance + "/DONE/DONE." + this.feedName + "." + instance;
            FileUtils.writeStringToFile(dstStatusFile, statusData);
        }
        
        // setup done file
        {
            File dstDoneFile = new File(tmpFeedPath + "DONE." + this.feedName + "." + instance);
            String doneData = "/tmp/FDIROOT/" + this.feedName + "/" + instance + "/DONE/" + this.feedName + "_0";
            FileUtils.writeStringToFile(dstDoneFile, doneData);
        }
        
        // setup data 
        {
            File srcDataFile = new File(sourceFeedPath + "generic_feed_data");
            File dstDataFile = new File(tmpFeedPath + this.feedName + "_0");
            FileUtils.copyFile(srcDataFile, dstDataFile);
        }
        
        // move to final directory
        {
            File finalDir = new File("/grid/0/yroot/var/yroots/fdiserver/tmp/FDIROOT/" + this.feedName + "/" + instance);
            FileUtils.forceMkdir(finalDir);
            FileUtils.copyDirectoryToDirectory(tmpFeedDir, finalDir);
        }
    }
}


