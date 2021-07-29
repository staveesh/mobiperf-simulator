package za.ac.uct.cs.mobiperfsimulator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import za.ac.uct.cs.mobiperfsimulator.Constants;
import za.ac.uct.cs.mobiperfsimulator.measurements.MeasurementTask;
import za.ac.uct.cs.mobiperfsimulator.orchestration.MeasurementScheduler;

import java.util.Date;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

@Configuration
public class MeasurementSchedulerConfig implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(MeasurementSchedulerConfig.class);

    @Autowired
    private MeasurementScheduler mScheduler;

    private Date nextRunTimeStamp;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setScheduler(jobSchedulerThread());
        taskRegistrar.addTriggerTask(() -> {
            PriorityBlockingQueue<MeasurementTask> queue = mScheduler.getTaskQueue();
            if (queue.size() > 0) {
                logger.info("Handling next measurement...");
                mScheduler.handleMeasurement();
            }
        }, triggerContext -> {
            nextRunTimeStamp = new Date(System.currentTimeMillis() +
                        TimeUnit.SECONDS.toMillis(Constants.JOB_SCHEDULER_PERIOD_SECONDS));
            PriorityBlockingQueue<MeasurementTask> queue = mScheduler.getTaskQueue();
            if (queue.size() > 0) {
                MeasurementTask nextTask = queue.peek();
                nextRunTimeStamp = new Date(System.currentTimeMillis() +
                        Math.max(nextTask.timeFromExecution(), Constants.MIN_TIME_BETWEEN_MEASUREMENT_ALARM_MSEC));
            }
            return nextRunTimeStamp;
        });
    }

    @Bean
    public TaskScheduler jobSchedulerThread() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("JobScheduler");
        scheduler.setPoolSize(100);
        scheduler.initialize();
        return scheduler;
    }
}
