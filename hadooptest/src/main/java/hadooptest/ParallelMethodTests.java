package hadooptest;

/**
 * JUnit 4.8 introduced the notion of Categories. You can use JUnit categories
 * by using the groups parameter. Categories are implemented as classes or
 * interfaces. 
 * 
 * NOTE: this was initially abstract out to the coretest project, so that it can
 * be shared by other test frameworks. However, maven parallel method execution
 * appears to have an issue when the category interface is coming from an
 * external project, such that it will not output any test logs until the test
 * has finished completely. For longer running tests, it is problematic as it 
 * give the impression that the test is hung, and also prevents debugging the
 * test execution in real time. For now, the workaround is to duplicate this
 * class here, and have the tests reference this version instead of the one from
 * coretest. 
 */
public interface ParallelMethodTests {}
