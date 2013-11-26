#!/usr/bin/env python
import unittest
import os
import sys
import re

class WidgetTestCase(unittest.TestCase):
    '''Small program to run through simple spot-checks of a\nhadoop-installation. This has been tested against 0.20.200-level\ninstallations, nothing higher/lower. Your mileage may vary.'''
    def setUp(self):
	self.reVersions = [
		re.compile("^Hadoop (\d+.\d+.\S+)"),
		re.compile("^Subversion (git://\S+.com/)")]

    def tearDown(self):
        # self.widget.dispose()
        # self.widget = None
	pass
    def testHadoopMapredHomeExists(self):
        '''testHadoopMapredHomeExists:\n\tcheck that $HADOOP_MAPRED_HOME is a dir.'''
        assert os.path.exists(hadoop_mapred_home), 'test that HADOOP_MAPRED_HOME exists.'
        assert os.path.isdir(hadoop_mapred_home), 'test that HADOOP_MAPRED_HOME is directory.'
    def testHadoopMapredHomeBinExists(self):
        '''testHadoopMapredHomeBinExists:\n\tcheck that $HADOOP_MAPRED_HOME/bin is a dir.'''
        assert os.path.exists(hadoop_mapred_home + "/bin"), 'test that HADOOP_MAPRED_HOME/bin exists.'
        assert os.path.isdir(hadoop_mapred_home + "/bin"), 'test that HADOOP_MAPRED_HOME/bin is directory.'
    def testHadoopHomeMapredBinHadoopExists(self):
        '''testHadoopMapredHomeBinHadoopExists:\n\tcheck that $HADOOP_MAPRED_HOME/bin/hadoop is shell script.'''
        assert os.path.exists(hadoop_mapred_home + "/bin/mapred"), 'test that bin/mapred exists.'
        assert os.path.isfile(hadoop_mapred_home + "/bin/mapred"), 'test that bin/mapred is plain-file.'
    def testHadoopHDFSHomeExists(self):
        '''testHadoopHDFSHomeExists:\n\tcheck that $HADOOP_HDFS_HOME is a dir.'''
        assert os.path.exists(hadoop_hdfs_home), 'test that HADOOP_HDFS_HOME exists.'
        assert os.path.isdir(hadoop_hdfs_home), 'test that HADOOP_HDFS_HOME is directory.'
    def testHadoopHDFSHomeBinExists(self):
        '''testHadoopHDFSHomeBinExists:\n\tcheck that $HADOOP_HDFS_HOME/bin is a dir.'''
        assert os.path.exists(hadoop_hdfs_home + "/bin"), 'test that HADOOP_HDFS_HOME/bin exists.'
        assert os.path.isdir(hadoop_hdfs_home + "/bin"), 'test that HADOOP_HDFS_HOME/bin is directory.'
    def testHadoopHomeHDFSBinHadoopExists(self):
        '''testHadoopHDFSHomeBinHadoopExists:\n\tcheck that $HADOOP_HDFS_HOME/bin/hadoop is shell script.'''
        assert os.path.exists(hadoop_hdfs_home + "/bin/hdfs"), 'test that bin/hdfs exists.'
        assert os.path.isfile(hadoop_hdfs_home + "/bin/hdfs"), 'test that bin/hdfs is plain-file.'
    def testHadoopCommonHomeExists(self):
        '''testHadoopCommonHomeExists:\n\tcheck that $HADOOP_COMMON_HOME is a dir.'''
        assert os.path.exists(hadoop_common_home), 'test that HADOOP_COMMON_HOME exists.'
        assert os.path.isdir(hadoop_common_home), 'test that HADOOP_COMMON_HOME is directory.'
    def testHadoopCommonHomeBinExists(self):
        '''testHadoopCommonHomeBinExists:\n\tcheck that $HADOOP_COMMON_HOME/bin is a dir.'''
        assert os.path.exists(hadoop_common_home + "/bin"), 'test that HADOOP_COMMON_HOME/bin exists.'
        assert os.path.isdir(hadoop_common_home + "/bin"), 'test that HADOOP_COMMON_HOME/bin is directory.'
    def testHadoopCommonHomeBinHadoopExists(self):
        '''testHadoopCommonHomeBinHadoopExists:\n\tcheck that $HADOOP_COMMON_HOME/bin/hadoop is shell script.'''
        assert os.path.exists(hadoop_home + "/bin/hadoop"), 'test that bin/hadoop exists.'
        assert os.path.isfile(hadoop_home + "/bin/hadoop"), 'test that bin/hadoop is plain-file.'
    def testHadoopCommonHomeBinHadoopRuns(self):
        '''testHadoopHomeBinHadoopRuns:\n\trun $HADOOP_COMMON_HOME/bin/hadoop version.'''
        fd = os.popen("%s/bin/hadoop version" % hadoop_common_home )
	lns = fd.readlines()
	fd.close()
	assert len(lns) > 0, "test that 'bin/hadoop version' has output."
	assert lns[0].startswith("Hadoop ")
	assert lns[1].startswith("Subversion ")
	for i in range(0,len(self.reVersions)):
		r = self.reVersions[i]
		m = r.match(lns[i])
		assert m is not None, "looking for match against " + lns[i]
    def testHadoopYMONDependenciesExist(self):
        '''testHadoopYMONDependenciesExist:\n\tlook for ymon .jar files that we need'''
        assert os.path.exists(hadoop_home), 'test that conf/hadoop exists.'
        for f in [ "axis.jar", "jaxrpc.jar", "saaj.jar", "wsdl4j-1.5.1.jar" ]:
            assert os.path.exists("%s/share/hadoop/common/lib/%s" % (hadoop_home, f)), 'test that hadoop-home/lib/%s exists.' % f
    def testHadoopConfigExists(self):
        '''testHadoopConfigExists:\n\trun $HADOOP_CONF_DIR is dir'''
        assert os.path.exists(hadoop_conf_dir), 'test that conf/hadoop exists.'
    def testHadoopConfigXmlLint(self):
        '''testHadoopConfigXmlLint:\n\trun xmllink on conf-dir (but not included files, yet'''
	ret = os.system("xmllint --noout  %s/*.xml" % hadoop_conf_dir)
        assert ret == 0, 'test that xmllint runs.'
    def testDoLogFilesHaveHostnameInFilename(self):
        '''testDoLogFilesHaveHostnameInFilename:\n\tlook for `hostname` in each log filename'''
        from socket import gethostname
        host = gethostname()
        for f in logFilesToInspect:
           assert f.find(host) > 0, "looking for %s in filename itself: logfile=%s" % (host,f)
    def testExamineAllLogFiles(self):
        '''testExamineAllLogFiles:\n\t\n\tlook for obvious-success and obvious-fail in all log files'''
        testcases = {}
        for f in logFilesToInspect:
           if f.find('namenode') > 0:
              testcases['positive'] = [ 'simon_dfs started',
                                 'simon_jvm started']
              testcases['negative'] =  [ 'Failure\.',
                                 'stopped\.',
                                 'Exception' ]
           elif f.find('jobtracker') > 0:
              testcases['positive'] = [ 'simon_mapred started',
                                 'simon_jvm started']
              testcases['negative'] =  [ 'Failure\.',
                                 'stopped\.',
                                 'Exception' ]
           elif f.find('historyserver') > 0:
              testcases['positive'] = [ 'JobHistoryServer: Started job history server at']
              testcases['negative'] =  [ 'Failure\.',
                                 'stopped\.',
                                 'Exception' ]
           elif f.find('datanode') > 0:
              testcases['positive'] = [ 'simon_dfs started',
                                 'simon_jvm started']
              testcases['negative'] =  [ 'Failure\.',
                                 'stopped\.',
                                 'Exception' ]
           elif f.find('tasktracker') > 0:
              testcases['positive'] = [ 'simon_mapred started',
                                 'simon_jvm started',
                                 'Logging to org.slf4j.impl.Log4jLoggerAdapter\(org.mortbay.log\) via org.mortbay.log.Slf4jLog']
              testcases['negative'] =  [ 'Failure\.',
                                 'stopped\.',
                                 'Exception' ]
           else:
              assert False, 'Did not recognize what logfile that %s is.' % f
           lns = open(f).readlines()
           contentsToCheck = "\n".join(lns)
           if testcases.has_key('positive'):
              for r in testcases['positive']:
                  m = re.search(r, contentsToCheck)
                  assert m is not None, "test that mandatory contents (%s) are included in %s, but they are not." % (r,f)
           if testcases.has_key('negative'):
              for r in testcases['negative']:
                  m = re.search(r, contentsToCheck)
                  assert m is None, "test that forbidden contents (%s) are not included in %s, but they are." % (r,f)
    def testHadoopConfigs(self):
        '''testHadoopConfigs:\n\tsimple tests against basic property values'''
        checkList = [
		[ 'hadoop.security.groups.cache.secs',
			lambda x: int(x) == 14400 ],
	]
	for (k,testProc) in checkList:
		testValue = configContents[k]
		assert testProc (  testValue), "checking that key=%s is correct. (value=%s failed)" % (k, testValue)



from xml.sax.handler import ContentHandler
from xml.sax import make_parser
class ImageHandler(ContentHandler):
    title = ""
    name = ""
    results = {}
    def startElement(self, name, attrs):
        self.CurrentData = name
        if (name == "property") :
            pass
    def characters(self, content):
        if self.CurrentData == "name":
           self.name = content
        if self.CurrentData == "value":
           self.value = content

    def endElement(self,name):
        if (name == "property") :
            self.results[self.name] = self.value
            self.name = self.value = "" # just for safety
        self.CurrentData = None

def pryPropertiesFromXML(xmlFile = "core-site.xml"):
    import sys
    
    # print "inspecting ", xmlFile
    
    image = ImageHandler()
    saxparser = make_parser()
    saxparser.setContentHandler(image)
    
    datasource = open(xmlFile,"r")
    saxparser.parse(datasource)
    # print "Just parsed %s"  % xmlFile 
    return image.results


	

# hadoop_home = os.environ.get( 'HADOOP_HOME', "/grid/0/gs/hadoop/current")
hadoop_common_home = os.environ.get( 'HADOOP_COMMON_HOME', "/grid/0/gs/???")
hadoop_mapred_home = os.environ.get( 'HADOOP_MAPRED_HOME', "/grid/0/gs/???")
hadoop_hdfs_home = os.environ.get( 'HADOOP_YARN_HOME', "/grid/0/gs/???")
hadoop_conf_dir = os.environ.get( 'HADOOP_CONF_DIR', "/grid/0/gs/conf/current")

perFileConfigContents = {}
configContents = {}
if __name__ == "__main__":

    import glob
    for f in glob.glob("%s/*.xml" % hadoop_conf_dir):
        if f.find("/EXAMPLE") < 0:
            # print "Processing %s" % f
            h = pryPropertiesFromXML(f)
            perFileConfigContents[f] = h
            for k in perFileConfigContents[f].keys():
                  configContents[k] = perFileConfigContents[f][k]
    logFilesToInspect = []
    expr = re.compile(".*\-jobtracker\-.*|.*\-historyserver\-.*|.*\-namenode\-.*|.*\-tasktracker\-.*|.*\-datanode\-.*")
    for d in hadoop_logs_dirs:
        for f in glob.glob("%s/*.log" % d):
           if expr.match(f) is not None: logFilesToInspect.append(f)
    if  len(logFilesToInspect) == 0:
       print "(note: assuming no log files to inspect. Is this a gateway machine?)"
            
    suite = unittest.TestLoader().loadTestsFromTestCase(WidgetTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
