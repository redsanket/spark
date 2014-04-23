package hadooptest.monitoring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Add this annotation to any test case to enable monitoring. The poll times for
 * CPU and memory can be configured, but not for the LogMonitor. This is because the
 * Log collection begins at the start of the test and logs are fetched once the
 * test completes.
 * </p>
 * 
 * @author tiwari
 * 
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Monitorable {
	int cpuPeriodicity() default 30;

	int memPeriodicity() default 30;
}
