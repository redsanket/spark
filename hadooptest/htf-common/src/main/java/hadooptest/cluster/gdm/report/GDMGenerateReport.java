package hadooptest.cluster.gdm.report;

import java.io.IOException;

import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * Used for annotation for each testcase that needs to be in report.
 * @author lawkp
 *
 */
public class GDMGenerateReport extends BlockJUnit4ClassRunner {

	public GDMGenerateReport(Class<?> klass) throws InitializationError {
		super(klass);
	}

	@Override 
	public void run(RunNotifier notifier){
		try {
			notifier.addListener(new GDMTestExecutionReport());
			super.run(notifier);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
