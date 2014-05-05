package hadooptest.hadoop.regression.yarn.capacityScheduler;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;

public class CustomShuffle extends Shuffle {
	public RawKeyValueIterator run() throws IOException, InterruptedException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		boolean keepLooping = true;
		while (keepLooping) {
			if (!fs.exists(new Path("/tmp/go"))) {
				System.out
						.println("------------- "
								+ "/tmp/go"
								+ " ----------- not found ---------- sleeping for 500 ms ---------");
				Thread.sleep(500);
			} else {
				keepLooping = false;
				System.out
						.println("+++++++++++++++ "
								+ "/tmp/go"
								+ " -----------  FOUND  ---------- continuing with normal shuffle ---------");
			}
		}

		return super.run();
	}
}
