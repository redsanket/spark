// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.api;

import static com.jayway.restassured.RestAssured.given;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.gdm.ConsoleHandle;
import hadooptest.cluster.gdm.HTTPHandle;
import hadooptest.cluster.gdm.report.GDMGenerateReport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.jayway.restassured.path.xml.XmlPath;
import com.jayway.restassured.response.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SerialTests.class)
public class GetDatasetsApiTest extends TestSession {
    
    private String cookie;
    private String url;
    private ConsoleHandle consoleHandle;
    private String sourceGrid;
    private String target;
    private String newDataSetName =  "TestDataSet_" + System.currentTimeMillis();
    private static final String INSTANCE1 = "20151201";
    private List<String>datasetsResultList;
    private List<String>dataSourceList= new ArrayList<String>();
    private List<String>dataTargetList= new ArrayList<String>();
    public static final String dataSetPath = "/console/query/config/dataset/getDatasets";
    public static final String dataSourcePath = "/console/query/config/datasource";
    
    @BeforeClass
    public static void startTestSession() throws Exception {
        TestSession.start();
    }
    
    @Before
    public void setUp() throws NumberFormatException, Exception {
        this.consoleHandle = new ConsoleHandle();
        
        //read console url 
        this.url =  this.consoleHandle.getConsoleURL();
        TestSession.logger.debug("url  = " + this.url);
        HTTPHandle httpHandle = new HTTPHandle();
        cookie = httpHandle.getBouncerCookie();
         
        datasetsResultList = getDataSetListing(cookie , this.url + this.dataSetPath).getBody().jsonPath().getList("DatasetsResult.DatasetName");
        if(datasetsResultList == null){
            fail("Failed to get the datasets");
        }
        
        if (datasetsResultList.size() == 0) {
        	// create the dataset.

        	List<String> grids = this.consoleHandle.getUniqueGrids();
        	if (grids.size() < 2) {
        		Assert.fail("Only " + grids.size() + " of 2 required grids exist");
        	}
        	this.sourceGrid = grids.get(0);
        	this.target = grids.get(1);
        	createDataset();

        	datasetsResultList = getDataSetListing(cookie , this.url + this.dataSetPath).getBody().jsonPath().getList("DatasetsResult.DatasetName");
        	assertTrue("Failed to get the newly created dataset name" , datasetsResultList.size() > 0);
        }
        
        // Invoke "/console/query/config/datasource" GDM REST API and collect all the source and target elements in Lists
        List<String>tempSource = Arrays.asList(getDataSetListing(cookie , this.url + this.dataSourcePath).getBody().prettyPrint().replace("/", "").split("datasource"));
        if (tempSource == null) {
            fail("Failed to get the data sources");
        }
        for (String str : tempSource) {
            if (str.contains("target")) {
                String temp[] = str.split(",");
                if (temp[0] != null  && temp[0] != "") {
                    dataTargetList.add( temp[0]);
                }
            } else {
                String temp[] = str.split(",");
                if (temp[0] != null  && temp[0] != "") {
                    dataSourceList.add(temp[0]);
                }
            }
        }
    }
    
    /**
     * Verify whether a given dataset is select and it is returned in response
     */
    @Test
    public void testDataSetExistInDatasetsResults() {
        String dataSetName = datasetsResultList.get(0);
        boolean flag = getResult(dataSetName);
        assertEquals(dataSetName + " dataset does not exists in " + datasetsResultList, flag, true );
    }
    
    /**
     * Verify whether dataset(s) are selected for a given regular expression.
     * Note: Regular expression starting with .(dot) or Matches any single character except newline. 
     */
    @Test
    public void testGetDataSetMatcherDotDataSetName() {
        String dataSetName = datasetsResultList.get(0);
        String regex = "."+dataSetName.substring(1);
        boolean flag = getResult(dataSetName);
        assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
    }
    
    /**
     * Verify whether dataset(s) are selected for a given regular expression.
     * Note:  Matches any single character not in brackets regular expression
     */
    @Test
    public void testGetDataSetMatcherMatchFirstCharacterInRangeDataSetName() {
        String dataSetName1 = datasetsResultList.get(0);
        String regex = "[^"+ dataSetName1.charAt(0)+"]";
        TestSession.logger.info("regex = "+regex);
        boolean flag = getResult(regex);
        assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
    }

    /**
     * Verify whether dataset(s) are selected for a given regular expression.
     * Note:  Matches regex exp1 or exp2 regular expression
     */
    @Test
    public void testGetDataSetMatcherORDataSetName() {
        String dataSetName = datasetsResultList.get(0);
        String regex = dataSetName + "|prq";
        TestSession.logger.info("regex = "+regex);
        boolean flag = getResult(regex);
        assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
    }
    
    /**
     * Verify whether dataset(s) are selected for a given regular expression.
     * Note : Matches any single character in brackets regular expression
     */
    @Test
    public void testGetDataSetMatcherAnyCharacterDataSetName() {
        String regex = "[ab]";
        TestSession.logger.info("regular expression = "+regex);
        boolean flag = getResult(regex);
        assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
    }
    
    /**
     * Verify whether dataset(s) are selected for a given regular expression.
     * Note : Matches any single character in a given regular expression from the beginning of the line (BOL)
     */
    @Test
    public void testGetDataSetMatcherBOLDataSetName() {
        String dataSetName = datasetsResultList.get(0);
        String regex = "^"+dataSetName.substring(0,3);
        TestSession.logger.info("regular expression = "+regex);
        boolean flag = getResult(regex);
        assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
    }
    
    /**
     * Verify whether dataset(s) are selected for a given regular expression.
     * Note : Matches end of line(EOL) regular expression
     */
    @Test
    public void testGetDataSetMatcherEOLDataSetName() {
        String dataSetName = datasetsResultList.get(0);
        String regex = dataSetName.substring(dataSetName.length()-1) +"$";
        TestSession.logger.info("regular expression = "+regex);
        boolean flag = getResult(regex);
        assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
    }
    
    // ==================================  source ========================

    /**
     * Verify whether a given dataset is selected and return the same in response.
     * Note: Regular expression starting with .(dot) or Matches any single character except newline.
     */
    @Test
    public void testGetDataSetMatcherDOTSource() {
        String sourceName = "gdm-fdi-source-patw02";
        TestSession.logger.info("sourceName   = "+sourceName);
        String regex = "."+sourceName.substring(1);
        TestSession.logger.info("regex = "+regex);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?source=" + regex);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = "+res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String name = getDataSource(ds, "source" );
            boolean flag = dataSourceList.contains(name);
            assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
        }
    }
    
    /**
     * Verify whether a  dataset(s) are selected for a given regular expression
     * Note : Matches beginning of line (BOL ) 
     */
    @Test
    public void testGetDataSetMatcherCapSource() {
        String sourceName = "gdm-fdi-source-patw02";
        String regex = "^"+ sourceName  ;
        TestSession.logger.info("regex = "+regex);
        Response response = given().cookie(cookie).get(url + dataSetPath +"?source=" + regex);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = "+res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String name = getDataSource(ds, "source" );
            boolean flag = dataSourceList.contains(name);
            assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
        }
    }
    
    /**
     * Verify whether a  dataset(s) are selected for a given regular expression
     * Note : Matches 0 or more occurrences of preceding regular expression. 
     */
    @Test
    public void testGetDataSetWithAstrictSource() {
        List<String> dsNameList = this.consoleHandle.getAllDataSetName();
        String temp = dsNameList.get(0);
        String regex =  temp + "*" ;
        TestSession.logger.info("regex = "+regex);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + regex);
        List<String> responseDataSetList = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("responseDataSetList  = " + responseDataSetList);
        if (responseDataSetList.size() > 0) {
            boolean flag = responseDataSetList.contains(temp);
            assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
        }
    }
    
    /**
     * Verify whether a  datasource(s) are selected for a given regular expression
     * Note : Matches end of line(EOL) regular expression 
     */
    @Test
    public void testGetDataSetDollarSource() {
        List<String> grids = this.consoleHandle.getUniqueGrids();
        String sourceName = grids.get(0);
        String regex =  sourceName.substring(0,sourceName.length() - 1) +"$";
        TestSession.logger.info("regex = "+regex);
        Response response = given().cookie(cookie).get(url + dataSetPath +"?dataset="+regex);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = "+res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String name = getDataSource(ds, "source" );
            boolean flag = dataSourceList.contains(name);
            assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
        }
    }
    
    // ==================================  target ========================
    
    /**
     * Verify whether a  dataset(s) are selected for a given regular expression
     * Note : Matches end of line regular expression 
     */
    @Test
    public void testGetDataSetDollarTarget() {
    	List<String> tempGridList = this.consoleHandle.getUniqueGrids();
        String targetName = tempGridList.get(0);
        String regex =  targetName.substring(targetName.length() - 1) +"$";
        TestSession.logger.info("regular expression = "+regex);
        Response response = given().cookie(cookie).get(url + dataSetPath +"?target="+regex);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = "+res);
        if(res.size() > 0){
            String ds = res.get(0);
            TestSession.logger.info("ds = "+ds);
            String name = getDataSource(ds, "target" );
            TestSession.logger.info("name  = "+name);
            boolean flag = tempGridList.contains(name);
            assertEquals("No dataset got selected for " + regex + datasetsResultList   , true , flag);
        }
    }
    
    // ==================================  dataset & source ========================

    /**
     * Verify whether a given dataset is selected and return the same in response.
     */
    @Test
    public void testGetDataSetWithDatasetAndSource() {
        String dataSetName = datasetsResultList.get(0);
        Response response = given().cookie(cookie).get(url + dataSetPath +"?dataset="+dataSetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = "+ res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String sname = getDataSource(ds, "source" );
            TestSession.logger.info("sname  = " + sname);
            response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + ds  + "&source=" + sname );
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            TestSession.logger.info("res  = " + res);
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response.
     * Note : Matches 0 or more occurrences of preceding expression.
     */
    @Test
    public void testGetDataSetWithDatasetAndSourceWithAstrictForDataset() {
        String dataSetName = datasetsResultList.get(0);
        String regex = dataSetName.substring(0, 3) + "*";
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + dataSetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String sname = getDataSource(ds, "source");
            TestSession.logger.info("sname  = " + sname);
            response = given().cookie(cookie).get(url + dataSetPath + "?dataset="+ ds  + "&source=" + sname );
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            TestSession.logger.info("res  = " + res);
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response.
     * Note : Matches 0 or more occurrences of preceding expression.
     */
    @Test
    public void testGetDataSetWithDatasetAndSourceWithAstrictForSource(){
        String dataSetName = datasetsResultList.get(0);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + dataSetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String sname = getDataSource(ds, "source" );
            TestSession.logger.info("sname  = "  + sname + "   regex =  " + sname.substring(0, 3) + "*");
            response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + ds + "&source=" + sname.substring(0, 3) + "*");
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response.
     * Note : Matches 0 or more occurrences of preceding expression.
     */
    @Test
    public void testGetDataSetWithDatasetAndSourceWithAstrictForSourceAndDataset(){
        String datasetName = datasetsResultList.get(0).substring(0, 3) + "*";
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String sname = getDataSource(ds, "source" );
            TestSession.logger.info("sname  = " + sname + "   regex =  " + sname.substring(0, 3) + "*");
            response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + ds + "&source=  " + sname.substring(0, 3) + "*");
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    // ==================================  dataset & target ========================

    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset and target are specified are parameter.
     */
    @Test
    public void testGetDataSetWithDatasetAndTarget() {
        String datasetName = datasetsResultList.get(0);
        TestSession.logger.info("datasetName  = " + datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath +"?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            TestSession.logger.info("tname  = "+tname);
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset=" + ds +" &target="+tname);
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset and target are specified are parameter.
     * Note : Dataset is specified as with regular expression ( * - Matches 0 or more occurrences of preceding expression ) 
     */
    @Test
    public void testGetDataSetWithDatasetAndTargetWithAstrictForDataset() {
        String datasetName = datasetsResultList.get(0).substring(0, 3) + "*";
        TestSession.logger.info("datasetName  = " + datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            TestSession.logger.info("tname  = " + tname);
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset=" + ds + "&target=" + tname);
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset and target are specified are parameter.
     * Note : Target is specified as with regular expression ( * - Matches 0 or more occurrences of preceding expression ) 
     */
    @Test
    public void testGetDataSetWithDatasetAndSourceWithAstrictForTarget() {
        String datasetName = datasetsResultList.get(0);
        TestSession.logger.info("datasetName  = " + datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            TestSession.logger.info("tname  = " + tname);
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset="+ ds + "&target=" + tname.substring(0, 3) + "*");
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }

    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset and target are specified are parameter.
     * Note : Dataset & Target are specified as with regular expression ( * - Matches 0 or more occurrences of preceding expression ) 
     */
    @Test
    public void testGetDataSetWithDatasetAndSourceWithAstrictForTargetAndDataset() {
        String datasetName = datasetsResultList.get(0).substring(0, 3) + "*";;
        TestSession.logger.info("datasetName  = "+datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath +"?dataset="+datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = "+res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            TestSession.logger.info("tname  = "+tname);
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset="+ ds +"&target="+tname.substring(0, 3) + "*");
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = "+dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    // ==================================  dataset, source &  target ========================
    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset , datasource and target are specified are parameter.
     */
    @Test
    public void testGetDataSetWithDatasetSourceAndTarget() {
        String datasetName = datasetsResultList.get(0);
        TestSession.logger.info("datasetName  = " + datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            String sname = getDataSource(ds, "target" );
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset=" + ds + "&target=" + tname + "&source=" + sname);
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }

    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset , datasource and target are specified are parameter.
     * Note : dataset is specified as regular expression ( * - Matches 0 or more occurrences of preceding expression)
     */
    @Test
    public void testGetDataSetWithDatasetSourceAndTargetWithAstrictForDataset() {
        String datasetName = datasetsResultList.get(0).substring(0, 3) + "*";
        TestSession.logger.info("datasetName  = "+datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            String sname = getDataSource(ds, "target" );
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset=" + ds + "&target=" + tname + "&source=" + sname);
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset , datasource and target are specified are parameter.
     * Note : source is specified as regular expression ( * - Matches 0 or more occurrences of preceding expression)
     */
    @Test
    public void testGetDataSetWithDatasetSourceAndTargetWithAstrictForTarget() {
        String datasetName = datasetsResultList.get(0);
        TestSession.logger.info("datasetName  = " + datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            String sname = getDataSource(ds, "target" );
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset="+ ds + "&target="+ tname.substring(0, 3) + "*" + "&source=" + sname);
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = "+dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset , datasource and target are specified are parameter.
     * Note : target and source are specified as regular expression ( * - Matches 0 or more occurrences of preceding expression)
     */
    @Test
    public void testGetDataSetWithDatasetSourceAndTargetWithAstrictForAllParameter() {
        String datasetName = datasetsResultList.get(0);
        TestSession.logger.info("datasetName  = " + datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            String sname = getDataSource(ds, "target" );
            String express = ds.substring(0, 3);
            response = given().cookie(cookie).get(url + dataSetPath  + "?dataset="+ express + "*" + "&target="+ tname.substring(0, 3) + "*" + "&source=" + sname.substring(0, 3) + "*");
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = "+ dsName);
                String result = dsName.substring(0,3);
                assertEquals("DataSetNames are not equal" , express , result );
            }
        }
    }
    
    /**
     * Verify whether a given dataset is selected and return the same in response, when both dataset , datasource and target are specified are parameter.
     * Note : source is specified as regular expression ( * - Matches 0 or more occurrences of preceding expression)
     */
    @Test
    public void testGetDataSetWithDatasetSourceAndTargetWithAstrictForSource() {
        String datasetName = datasetsResultList.get(0);
        TestSession.logger.info("datasetName  = "+datasetName);
        Response response = given().cookie(cookie).get(url + dataSetPath + "?dataset=" + datasetName);
        List<String> res = response.jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("res  = " + res);
        if (res.size() > 0) {
            String ds = res.get(0);
            String tname = getDataSource(ds, "source" );
            String sname = getDataSource(ds, "target" );
            String testURL = url + dataSetPath  + "?dataset="+ ds + "&target=" + tname + "&source=" + sname.substring(0, 3) + "*";
            TestSession.logger.info("testURL - " + testURL);
            response = given().cookie(cookie).get(testURL);
            res = response.jsonPath().getList("DatasetsResult.DatasetName");
            TestSession.logger.info("---- res - " + res);
            if (res.size() > 0) {
                String dsName = res.get(0);
                TestSession.logger.info("ds  = " + dsName);
                assertEquals("DataSetNames are not equal" , ds , dsName );
            }
        }
    }
    
    /**
     * Method that invokes "/console/query/config/dataset/getDatasets" GDM REST API, which accepts regular expression as parameter.
     * After getting the response, the response is converted to List<String> where each element passes through the regular expression that was 
     * passed as argument for the REST API. This to make sure that response got from the REST API is correct. Finally the elements are collected and once again
     * matched with the pre populated List that was constructed in BeforeClass method  
     * @param regex
     * @return
     */
    private boolean getResult(String regex) {
        boolean flag = false;
        
        // Contruct a request URL for the given regular expression 
        String testURL = this.url + dataSetPath + "?dataset=" + regex;
        
        // Select DataSetName item from the response and construct a List
        List<String> dataSetNamesList = given().cookie(cookie).get(testURL).getBody().jsonPath().getList("DatasetsResult.DatasetName");
        TestSession.logger.info("reponse = " + dataSetNamesList);
        List<String> matchList = matchAndFillList(this.datasetsResultList , regex);
        flag = matchForEqual(dataSetNamesList, matchList);
        return flag;
    }

    
    /**
     * Compare each elements in List to pass through the specified regular expression 
     * @param datasetsResult
     * @param regex
     * @return
     */
    private List<String> matchAndFillList(List<String>datasetsResult , String regex) {
        List<String> list = new ArrayList<String>();
        Pattern p = Pattern.compile(regex);
        Iterator<String> itr = datasetsResult.iterator();
        while (itr.hasNext()) {
            String dataSet = itr.next();
            Matcher m = p.matcher(dataSet);
            if (m.find()) {
                list.add(dataSet);
            }
        }
        return list;
    }

    /**
     * Compare two DataSet List & if both matches return true else return false
     * @param responeList
     * @param matchList
     * @return
     */
    private boolean matchForEqual(List<String>responeList , List<String>matchList) {
        boolean matchFound = false;
        int count = 0;
        if (responeList != null && matchList != null && responeList.size() == matchList.size()) {
            Iterator<String> itr = matchList.iterator();
            while (itr.hasNext()) {
                if (responeList.contains(itr.next())) {
                    count++;
                }
            }
            if (count == responeList.size() && count == matchList.size()) {
                matchFound = true;
            }
        }
        return matchFound;
    }
    
    /**
     * Method that returns attribute name for the given tag
     * @param dataSource - dataSource specification name
     * @param sourceType - specify whether its source or target tag for which it has to return the attribute.
     * @return
     */
    private String getDataSource(String dataSource , String sourceType ) {
        String xml = given().cookie(cookie).get(url + "/console/query/config/dataset/"+dataSource).andReturn().asString();
        XmlPath xmlPath = new XmlPath(xml);
        xmlPath.setRoot("DataSet");
        int size = 0;
        if (sourceType.equals("source")) {
            size = xmlPath.get("Sources.Source.size()");
        } else if (sourceType.equals("target")) {
            size = xmlPath.get("Targets.Target.size()");
        }
        boolean flag = false;
        String name = null;
        for (int i=0;i<= size - 1 ; i++) {
            if (sourceType.equals("source")) {
                name = xmlPath.getString("Sources.Source[" + i + "].@name");
                break;
            } if (sourceType.equals("target")) {
                name = xmlPath.getString("Targets.Target[" + i + "].@name");
                flag = dataTargetList.contains(name);
            }
        }
        return name;
    }
    
    private com.jayway.restassured.response.Response getDataSetListing(String cookie , String url)  {
        com.jayway.restassured.response.Response response = given().cookie(cookie).get(url );
        return response;
    }
    
    private void createDataset() {
        String basePath = "/projects/" + this.newDataSetName + "/data/%{date}";
        String dataSetConfigFile = Util.getResourceFullPath("gdm/datasetconfigs/BasicReplDataSet1Target.xml");
        String dataSetXml = this.consoleHandle.createDataSetXmlFromConfig(this.newDataSetName, dataSetConfigFile);
        dataSetXml = dataSetXml.replaceAll("TARGET", this.target);
        dataSetXml = dataSetXml.replaceAll("NEW_DATA_SET_NAME", this.newDataSetName);
        dataSetXml = dataSetXml.replaceAll("SOURCE", this.sourceGrid );
        dataSetXml = dataSetXml.replaceAll("START_TYPE", "fixed" );
        dataSetXml = dataSetXml.replaceAll("START_DATE", INSTANCE1);
        dataSetXml = dataSetXml.replaceAll("END_TYPE", "offset");
        dataSetXml = dataSetXml.replaceAll("END_DATE", "0");
        dataSetXml = dataSetXml.replaceAll("REPLICATION_DATA_PATH", basePath);
        dataSetXml = dataSetXml.replaceAll("CUSTOM_PATH_DATA", basePath);
        hadooptest.cluster.gdm.Response response = this.consoleHandle.createDataSet(this.newDataSetName, dataSetXml);
        if (response.getStatusCode() != HttpStatus.SC_OK) {
            Assert.fail("Response status code is " + response.getStatusCode() + ", expected 200.");
        }
        
        this.consoleHandle.sleep(30000);
    }
}
