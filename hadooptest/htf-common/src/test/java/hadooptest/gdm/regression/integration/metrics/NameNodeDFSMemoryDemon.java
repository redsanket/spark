// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.integration.metrics;

import hadooptest.TestSession;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class NameNodeDFSMemoryDemon {

    public NameNodeDFSMemoryDemon() {
        TestSession.logger.info("starting NameNodeDFSMemoryDemon....!");
    }

    public boolean startNameNodeDFSMemoryDemon() {
        boolean flag = false;
        try {

            // specify the job' s details..
            JobDetail job = JobBuilder.newJob(NameNodeDFSMemoryJob.class)
                    .withIdentity("NameNodeDFSMemoryDemon")
                    .build();

            // specify the running period of the job
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInSeconds(10)
                            .repeatForever())
                            .build();

            //schedule the job
            SchedulerFactory schFactory = new StdSchedulerFactory();
            Scheduler sch = schFactory.getScheduler();
            sch.start();
            sch.scheduleJob(job, trigger);
            flag = true;
            
        } catch (SchedulerException e) {
            e.printStackTrace();

        }
        return flag;
    }

}
