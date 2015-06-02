package hadooptest.storm;

import com.yahoo.spout.http.Config;
import com.yahoo.spout.http.RegistryStub;
import com.yahoo.spout.http.Util;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.JsonNode;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.http.HttpFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.Argument;

public class DRPCClient extends Thread {
    final static Logger LOG = LoggerFactory.getLogger(DRPCClient.class);
    URI serviceURI;
    String serviceID;
    HttpClient httpClient;
    int messageSize;
    int numMessages;
    int instance;
    boolean validate = false;
    String[] ycaV1Roles;
    String[] drpcURIs;
    long startTime = -1;
    long[][] starts;
    long[][] durations;
    long[][] ends;
    long endTime = -1;
    static long programStart = System.currentTimeMillis();


    private static class DRPCClientMainArgs {
        @Option(name="--help", aliases={"-h"}, usage="print help message")
        boolean help = false;

        @Option(name="--num_threads", aliases={"-t"}, usage="number of threads to run in parallel")
        Integer num_threads = 1; 

        @Option(name="--num_messages", aliases={"-n"}, usage="number of messages to send on each thread.  -1 is forever")
        Integer num_messages = 1; 

        @Option(name="--message_size", aliases={"-s"}, usage="Size of message to send.  Default 1k")
        Integer message_size = 1024; 

        @Option(name="--http_cert_filename", aliases={"-c"}, usage="the SSL cert to use to connect to the https port")
        String http_cert_filename = null;

        @Option(name="--yca_v1_role", aliases={"-y"}, usage="YCA V1 role to use for authentication" )
        String yca_v1_role = null; 

        @Option(name="--drpc_uri", aliases={"-d"}, usage="Comma seperated list of drpc servers URIs to connect to" )
        String drpc_uri = null; 

        @Option(name="--result_dir", aliases={"-r"}, usage="Directory to save off result files data.0, data.1, etc in csv format" )
        String result_dir = "results"; 

        @Option(name="--validate", aliases={"-v"}, usage="Validate returned data." )
        String validate = "false"; 

        @Argument
        List<String> args = new ArrayList<String>();
    }

    /**
     * @param args: 
     */
    public static void main(String[] args) throws Exception {
        DRPCClientMainArgs parsedArgs = new DRPCClientMainArgs();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        boolean validate = false;
        parser.setUsageWidth(80);
        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            parsedArgs.help = true;
        }

        if (parsedArgs.help) {
            System.err.println("cmd [options] ");
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        if (!parsedArgs.validate.equals("false")) {
            validate = true;
        }

        if (parsedArgs.yca_v1_role == null || parsedArgs.drpc_uri == null || parsedArgs.http_cert_filename == null) {
            System.err.println("Need to specify yca_v1_role, drpc_uri, and http_cert_filename");
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        // Make sure that the number of drpc_uris match the number of roles and the number of cert files.
        String [] certFiles = parsedArgs.http_cert_filename.split(",");
        String [] drpcURIs = parsedArgs.drpc_uri.split(",");
        String [] ycaV1Roles = parsedArgs.yca_v1_role.split(",");
        
        if (certFiles.length != drpcURIs.length || drpcURIs.length != ycaV1Roles.length) {
            System.err.println("Comma separated list of drpc uris, cert files, and v1 Roles need to be the same size and need to correspond to one another");
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        // Set up the SSL Trust Store for Certs
        setUpTrustStore( certFiles, drpcURIs );
        ArrayList<DRPCClient> dcList = new ArrayList<DRPCClient>();
	    for (int i = 0 ; i < parsedArgs.num_threads ; i++ ) {
            DRPCClient dc = new DRPCClient(parsedArgs.message_size, parsedArgs.num_messages, ycaV1Roles, drpcURIs, i, validate );
            try {
                dc.start();
            } catch (Exception e) {
                LOG.warn("Got an exception when starting thread " + i, e);
            }
            dcList.add( dc );
	    }

        // Make sure that we can write to the result directory
        File theDir = new File(parsedArgs.result_dir);
        if (!theDir.exists()) {
            System.out.println(parsedArgs.result_dir +" does not exist.   Creating");
            try {
                theDir.mkdir();
            } catch (Exception e) {
                LOG.warn("Could not create result dir" + parsedArgs.result_dir, e );
                parser.printUsage(System.err);
                System.err.println();
                return;
            }
        }

        String dirForResults= new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss").format(new Date());
        dirForResults = dirForResults + String.format("%03d", parsedArgs.num_threads);
        LOG.info("Creating result dir " + dirForResults);
        File resultDir = new File( parsedArgs.result_dir, dirForResults);
        try {
            resultDir.mkdir();
        } catch(Exception e) {
            LOG.warn("Could not write to result dir" + parsedArgs.result_dir, e );
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

	    // Wait for them all to finish
        for (int i = 0 ; i < parsedArgs.num_threads ; i++ ) {
            DRPCClient dc = dcList.get(i);
            try {
                dc.join();
            } finally {
                dc.halt();
            }
            LOG.info("Finished a halt on " + i );
        }

        // Now that the threads are all done, write out results
        for (int i = 0 ; i < parsedArgs.num_threads ; i++ ) {
            DRPCClient dc = dcList.get(i);
            long threadDuration = dc.endTime - dc.startTime;
            LOG.info("Duration for the thread was " + threadDuration);
            double totalInPost = 0;
            for (int function = 0 ; function < drpcURIs.length ; function++) {
                for (int count = 0 ; count < parsedArgs.num_messages ; count++) {
                    totalInPost += dc.durations[function][count];
                }
            }
            double avgInPost = totalInPost  / ((double) drpcURIs.length * (double) parsedArgs.num_messages);
            LOG.info("Avg time in each post was " + avgInPost);
            try {
                dc.writeResultsForThread( resultDir.getAbsolutePath(), i );
            } catch (Exception e) {
                LOG.warn("Could not write result for thread " + i, e);
            }
        }
        try {
            writeResultsForAllThreads( resultDir.getAbsolutePath(), dcList);
        } catch (Exception e) {
                LOG.warn("Could not write results  ", e);
        }
        LOG.info("ALL DONE!!");
    }

    public static void writeResultsForAllThreads( String resultDir, ArrayList<DRPCClient> threads ) throws Exception {
        String filename = "all.csv";
        File toWrite = new File( resultDir, filename );
        FileWriter writer = new FileWriter(toWrite.getPath());

        // Write out column headers
        writer.append("Thread"); writer.append(',');
        writer.append("Iteration"); writer.append(',');
        writer.append("Function"); writer.append(',');
        writer.append("Start"); writer.append(',');
        writer.append("End"); writer.append(',');
        writer.append("Duration"); writer.append('\n');
        for ( Integer thread = 0 ; thread < threads.size() ; thread++ ) {
            DRPCClient dc = threads.get(thread);
            for ( Integer i = 0 ; i < dc.numMessages ; i++ ) {
                for ( Integer j = 0 ; j < dc.ycaV1Roles.length ; j++ ) {
                    writer.append( thread.toString() ); writer.append( ',');
                    writer.append( i.toString() ); writer.append( ',');
                    writer.append( dc.drpcURIs[j] ); writer.append( ',');
                    writer.append( String.valueOf(dc.starts[j][i] - programStart ) ); writer.append( ',' );
                    writer.append( String.valueOf(dc.ends[j][i] - programStart ) ); writer.append( ',' );
                    writer.append( String.valueOf(dc.durations[j][i]) ); writer.append( '\n' );
                }
            }
        }
        writer.flush();
        writer.close();
    }


    static String oldPassword;
    static String oldKeyPassword;
    static String oldTrustStorePath;

    public static void revertTrustStore( ) throws Exception {
        if ( oldPassword == null ) {
            LOG.info("Clearing org.eclipse.jetty.ssl.password");
            System.clearProperty("org.eclipse.jetty.ssl.password");
        } else {
            LOG.info("Old password is " + oldPassword);
            System.setProperty("org.eclipse.jetty.ssl.password", oldPassword);
        }
        if ( oldKeyPassword == null ) {
            LOG.info("Clearing org.eclipse.jetty.ssl.keypassword");
            System.clearProperty("org.eclipse.jetty.ssl.keypassword");
        } else {
            LOG.info("Old keypassword is " + oldKeyPassword);
            System.setProperty("org.eclipse.jetty.ssl.keypassword", oldKeyPassword);
        }
        if ( oldTrustStorePath == null ) {
            LOG.info("Clearing javax.net.ssl.trustStore");
            System.clearProperty("javax.net.ssl.trustStore");
        } else {
            LOG.info("Old trustStorePath = " + oldTrustStorePath);
            System.setProperty("javax.net.ssl.trustStore", oldTrustStorePath);
        }
    }

    public static void setUpTrustStore( String[] certFiles, String[] serviceURLs ) throws Exception {

        // Save off old values
        oldPassword = System.getProperty("org.eclipse.jetty.ssl.password");
        oldKeyPassword = System.getProperty("org.eclipse.jetty.ssl.keypassword");
        oldTrustStorePath = System.getProperty("javax.net.ssl.trustStore");

        // Set up passwords
        String ssl_pwd = new BigInteger(130, new SecureRandom()).toString(32);
        System.setProperty("org.eclipse.jetty.ssl.password", ssl_pwd);
        System.setProperty("org.eclipse.jetty.ssl.keypassword", ssl_pwd);


        //create an empty trust store
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, ssl_pwd.toCharArray());
        FileInputStream fis = null;
        ByteArrayInputStream bais = null;

        LOG.info("KeyStore password is " + ssl_pwd.toCharArray());

        for (int i = 0 ; i < certFiles.length ; i++) {
            LOG.info("Creating a trust store for cert file " + certFiles[i] + " and service URL " + serviceURLs[i]);
            // use FileInputStream to read the file
            fis = new FileInputStream(certFiles[i]);

            // read the bytes
            byte value[] = new byte[fis.available()];
            fis.read(value);
            bais = new ByteArrayInputStream(value);

            // Create the cert factory and import the cert from input stream
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate certificate = factory.generateCertificate(bais);

            // Close input stream
            bais.close();
            fis.close();

            //import certificate into trustStore
            URI myServiceURI = new URI(serviceURLs[i]);
            String serviceID = Util.ServiceURItoID(myServiceURI);
            LOG.info("Adding Cert to trust store");
            trustStore.setCertificateEntry(serviceID, certificate);
        }

        //Store truststore into a file
        File truststore_fs = File.createTempFile("truststore", KeyStore.getDefaultType());
        String truststore_path = truststore_fs.getAbsolutePath();
        LOG.info("Trustore filename is " + truststore_path );
        trustStore.store(new FileOutputStream(truststore_fs), ssl_pwd.toCharArray());
        System.setProperty("javax.net.ssl.trustStore", truststore_path);
    }

    /**
     * Initialization
     * Get certificate from Service Registry for a specific service
     * Set up a SSL trust store
     * Set up cipher
     * Set up HTTPS client
     */
    public DRPCClient(int messageSize, int numMessages, String[] ycaV1Roles, String[] drpcURIs, int instance, boolean validate ) throws Exception {
        this.messageSize = messageSize;
        this.numMessages = numMessages;
        this.ycaV1Roles=ycaV1Roles;
        this.drpcURIs=drpcURIs;
        this.instance=instance;
        this.validate=validate;

        LOG.info("Creating a new thread to connect to " + drpcURIs);

        //HTTPS client
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setIncludeProtocols("TLSv1.2", "TLSv1.1", "TLSv1");
        sslContextFactory.setExcludeCipherSuites("SSL_RSA_WITH_RC4_128_MD5", "SSL_RSA_WITH_RC4_128_SHA");
        httpClient = new HttpClient(sslContextFactory);

        //start HTTP client
        httpClient.start();

        //set up storage for POST durations
        starts = new long[ycaV1Roles.length][];
        durations = new long[ycaV1Roles.length][];
        ends = new long[ycaV1Roles.length][];
        for ( int i = 0 ; i < ycaV1Roles.length ; i++ ) {
            starts[i] = new long[numMessages > 0 ? numMessages : 1]; 
            durations[i] = new long[numMessages > 0 ? numMessages : 1]; 
            ends[i] = new long[numMessages > 0 ? numMessages : 1]; 
        }
    }

    public void writeResultsForThread( String resultDir, int threadNumber ) throws Exception {
        String filename = "data." + String.format("%03d", threadNumber) + ".csv";
        File toWrite = new File( resultDir, filename );
        FileWriter writer = new FileWriter(toWrite.getPath());

        // Write out column headers
        writer.append("Iteration"); writer.append(',');
        writer.append("Function"); writer.append(',');
        writer.append("Start"); writer.append(',');
        writer.append("End"); writer.append(',');
        writer.append("Duration"); writer.append('\n');
        for ( Integer i = 0 ; i < numMessages ; i++ ) {
            for ( Integer j = 0 ; j < ycaV1Roles.length ; j++ ) {
                writer.append( i.toString() ); writer.append( ',');
                writer.append( drpcURIs[j] ); writer.append( ',');
                writer.append( String.valueOf(starts[j][i] - programStart ) ); writer.append( ',' );
                writer.append( String.valueOf(ends[j][i] - programStart ) ); writer.append( ',' );
                writer.append( String.valueOf(durations[j][i]) ); writer.append( '\n' );
            }
        }
        writer.flush();
        writer.close();
    }

    @Override
    public void run() {
        try {
            httpClient.setIdleTimeout(30000);
        } catch (Exception e) {  
                LOG.warn("Received exception on setIdleTimeout.  Exiting thread", e );
                return;
        }
        byte[] bytes = new byte[messageSize];
        Arrays.fill(bytes, (byte)('a' + instance));
        String[] v1Certs = new String[ycaV1Roles.length];
        for ( int i = 0 ; i < ycaV1Roles.length ; i++ ) {
            LOG.info("Attempting to connect to drpc server with " + drpcURIs[i]);
            try {
                v1Certs[i] = hadooptest.TestSessionStorm.getYcaV1Cert(ycaV1Roles[i]);
            } catch (Exception e) {
                LOG.warn("Could not get v1 cert for " + ycaV1Roles[i], e);
                return;
            }
        }

        int tryCount = numMessages;
        startTime = System.currentTimeMillis();
        while (tryCount < 0 || tryCount > 0) {
            for ( int i = 0 ; i < v1Certs.length ; i++ ) {
                ContentResponse postResp = null;

                try {
                    long postStart = System.currentTimeMillis();
                    if (tryCount > 0)
                        starts[i][numMessages - tryCount] = System.currentTimeMillis();
                    postResp = httpClient.POST(drpcURIs[i]).content(new BytesContentProvider(bytes), "text/plain").header("Yahoo-App-Auth", v1Certs[i]).send();
                    if (tryCount > 0) {
                        ends[i][numMessages - tryCount] = System.currentTimeMillis();
                        durations[i][numMessages - tryCount] = ends[i][numMessages - tryCount] - starts[i][numMessages - tryCount];
                    }
                } catch (Exception e) {
                    LOG.warn("Received exception on Post.  Exiting thread", e );
                    return;
                }

                if (postResp == null) {
                    LOG.warn("Post response was null.  Exiting thread.");
                    return;
                }

                if (postResp.getStatus() != 200) {
                    LOG.warn("Received invalid status on Post. Status is " + postResp.getStatus() );
                    return;
                }

                if (validate) {
                    if (postResp.getContent().length != bytes.length) {
                        LOG.warn("Returned size not equal to sent size ");
                        return;
                    }
                    if (!postResp.getContent().equals(bytes)) {
                        LOG.warn("Returned bytes not equal to sent bytes");
                        return;
                    }
                    LOG.info("Bytes validated" );
                }
            }

            if (tryCount > 0) {
                tryCount = tryCount - 1;
            }
        }
        endTime = System.currentTimeMillis();
        LOG.info("Finished run");
    }

    @Override
    protected void finalize() throws Exception {
        LOG.info("finalize:  About to halt");
        halt();
        LOG.info("finalize:  halted");
    }

    public void halt() throws Exception {
        LOG.info("halt:  About to stop");
        httpClient.stop();
        stop();
        LOG.info("halt:  stopped");
    }
}
