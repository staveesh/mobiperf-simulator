package za.ac.uct.cs.mobiperfsimulator.orchestration;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import za.ac.uct.cs.mobiperfsimulator.Constants;
import za.ac.uct.cs.mobiperfsimulator.measurements.MeasurementDesc;
import za.ac.uct.cs.mobiperfsimulator.measurements.MeasurementError;
import za.ac.uct.cs.mobiperfsimulator.measurements.MeasurementResult;
import za.ac.uct.cs.mobiperfsimulator.measurements.MeasurementTask;
import za.ac.uct.cs.mobiperfsimulator.util.MeasurementJsonConvertor;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

@Component
public class MeasurementScheduler {

    @Value("${mobiperf.schedule.file.path}")
    private String scheduleFilePath;

    private static final Logger logger = LoggerFactory.getLogger(MeasurementScheduler.class);

    private ExecutorService measurementExecutor;
    private volatile PriorityBlockingQueue<MeasurementTask> taskQueue;
    private volatile ConcurrentHashMap<MeasurementTask, Future<MeasurementResult>> pendingTasks;
    private final Hashtable<String, MeasurementTask> currentSchedule;
    private MeasurementTask currentTask;

    public MeasurementScheduler() {
        measurementExecutor = Executors.newSingleThreadExecutor();
        taskQueue = new PriorityBlockingQueue<>(Constants.MAX_TASK_QUEUE_SIZE,
                new TaskComparator());
        pendingTasks = new ConcurrentHashMap<>();
        this.currentSchedule =
                new Hashtable<>(Constants.MAX_TASK_QUEUE_SIZE);
    }

    /**
     * Load the schedule from the schedule file, if it exists.
     * <p>
     * This is to be run when the app first starts up, so scheduled items
     * are not lost.
     */
    @PostConstruct
    private void loadSchedulerState() {
        Vector<MeasurementTask> tasksToAdd = new Vector<MeasurementTask>();
        synchronized (currentSchedule) {
            try {
                logger.info("Restoring schedule from disk...");
                File scheduleFile = new File(scheduleFilePath);
                if(!scheduleFile.exists())
                    scheduleFile.createNewFile();
                FileInputStream inputstream = new FileInputStream(scheduleFile);
                InputStreamReader streamreader = new InputStreamReader(inputstream);
                BufferedReader bufferedreader = new BufferedReader(streamreader);

                String line;
                while ((line = bufferedreader.readLine()) != null) {
                    JSONObject jsonTask;
                    try {
                        jsonTask = new JSONObject(line);
                        MeasurementTask newTask =
                                MeasurementJsonConvertor.makeMeasurementTaskFromJson(jsonTask);

                        // If the task is scheduled in the past, re-schedule it in the future
                        // We assume tasks in the past have run, otherwise we can wind up getting
                        // stuck trying to run a large backlog of tasks

                        long curtime = System.currentTimeMillis();
                        if (curtime > newTask.getDescription().startTime.getTime()) {
                            long timediff = curtime - newTask.getDescription().startTime.getTime();

                            timediff = (long) (timediff % (newTask.getDescription().intervalSec * 1000));
                            Calendar now = Calendar.getInstance();
                            now.add(Calendar.SECOND, (int) timediff / 1000);
                            newTask.getDescription().startTime.setTime(now.getTimeInMillis());
                            logger.info("Rescheduled task " + newTask.getDescription().key +
                                    " at time " + now.getTimeInMillis());
                        }

                        tasksToAdd.add(newTask);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
                bufferedreader.close();
                streamreader.close();
                inputstream.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        updateSchedule(tasksToAdd, true);
    }

    public void updateSchedule(List<MeasurementTask> newTasks, boolean reLoad) {

        // Keep track of what tasks need to be added.
        // Altered tasks are removed and then added, so they go here too
        Vector<MeasurementTask> tasksToAdd = new Vector<MeasurementTask>();

        // Keep track of what keys are not being used. Remove keys from this as
        // you find they are in use.
        Set<String> missingKeys = new HashSet<String>(currentSchedule.keySet());
        Set<String> keysToRemove = new HashSet<String>();

        logger.info("Attempting to add new tasks");

        for (MeasurementTask newTask : newTasks) {

            String newKey = newTask.getDescription().key;
            if (!missingKeys.contains(newKey)) {
                tasksToAdd.add(newTask);
            } else {
                // check for changes. If any parameter changes, it counts as a change.
                // If there's a change, replace the task with the new task from the server
                keysToRemove.add(newKey);
                tasksToAdd.add(newTask);
                // We've seen the task
                missingKeys.remove(newKey);
            }
        }

        // scheduleKeys now contain all keys that do not exist
        keysToRemove.addAll(missingKeys);

        // Add all new tasks, and copy all unmodified tasks, to a new queue.
        // Also update currentSchedule accordingly.
        PriorityBlockingQueue<MeasurementTask> newQueue =
                new PriorityBlockingQueue<MeasurementTask>(Constants.MAX_TASK_QUEUE_SIZE,
                        new TaskComparator());

        synchronized (currentSchedule) {
            logger.info("Tasks to remove:" + keysToRemove.size());
            for (MeasurementTask task : this.taskQueue) {
                String taskKey = task.getDescription().key;
                if (!keysToRemove.contains(taskKey)) {
                    newQueue.add(task);
                } else {
                    logger.warn("Removing task with key" + taskKey);
                    // Also need to keep our master schedule up to date
                    currentSchedule.remove(taskKey);
                }
            }
            this.taskQueue = newQueue;
            // add all new tasks
            logger.info("New tasks added:" + tasksToAdd.size());
            for (MeasurementTask task : tasksToAdd) {
                submitTask(task);
                currentSchedule.put(task.getDescription().key, task);
            }
        }

        if (!reLoad && (!tasksToAdd.isEmpty() || !keysToRemove.isEmpty())) {
            saveSchedulerState();
        }
    }

    public boolean submitTask(MeasurementTask task) {
        try {
            // Immediately handles measurements created by user
            if (task.getDescription().priority == MeasurementTask.USER_PRIORITY) {
                return this.taskQueue.add(task);
            }

            if (taskQueue.size() >= Constants.MAX_TASK_QUEUE_SIZE
                    || pendingTasks.size() >= Constants.MAX_TASK_QUEUE_SIZE) {
                return false;
            }
            // Automatically notifies the scheduler waiting on taskQueue.take()
            return this.taskQueue.add(task);
        } catch (NullPointerException e) {
            logger.error("The task to be added is null");
            return false;
        } catch (ClassCastException e) {
            logger.error("cannot compare this task against existing ones");
            return false;
        }
    }

    /**
     * Save the entire current schedule to a file, in JSON format, like how
     * tasks are received from the server.
     * <p>
     * One item per line.
     */
    private void saveSchedulerState() {
        synchronized (currentSchedule) {
            try {
                BufferedOutputStream writer =
                        new BufferedOutputStream(new FileOutputStream(scheduleFilePath));

                logger.info("Saving schedule to a file...");
                for (Map.Entry<String, MeasurementTask> entry : currentSchedule
                        .entrySet()) {
                    try {
                        JSONObject task =
                                MeasurementJsonConvertor.encodeToJson(entry.getValue()
                                        .getDescription());
                        String taskstring = task.toString() + "\n";
                        writer.write(taskstring.getBytes());
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void handleMeasurement() {
        try {
            MeasurementTask task = taskQueue.peek();
            // Process the head of the queue.
            if (task != null && task.timeFromExecution() <= 0) {
                taskQueue.poll();
                Future<MeasurementResult> future;
                logger.info("Processing task " + task.toString());
                logger.info("Scheduling task:\n" + task);
                future =
                        measurementExecutor.submit(new PowerAwareTask(task,
                                this));
                synchronized (pendingTasks) {
                    pendingTasks.put(task, future);
                }

                MeasurementDesc desc = task.getDescription();

                long newStartTime =
                        desc.startTime.getTime() + (long) desc.intervalSec * 1000;

                // Add a clone of the task if it's still valid.
                if (newStartTime < desc.endTime.getTime()
                        && (desc.count == MeasurementTask.INFINITE_COUNT || desc.count > 1)) {
                    MeasurementTask newTask = task.clone();
                    if (desc.count != MeasurementTask.INFINITE_COUNT) {
                        newTask.getDescription().count--;
                    }
                    newTask.getDescription().startTime.setTime(newStartTime);
                    submitTask(newTask);
                }
            }
            // Schedule the next measurement in the taskQueue
            task = taskQueue.peek();
        } catch (IllegalArgumentException e) {
            // Task creation in clone can create this exception
            logger.error("Exception when cloning task");
            logger.info("Exception when cloning task: " + e);
        } catch (Exception e) {
            // We don't want any unexpected exception to crash the process
            logger.error("Exception when handling measurements", e);
            logger.info("Exception running task: " + e);
        }
    }

    /**
     * A task wrapper that is power aware, the real logic is carried out by realTask
     *
     * @author wenjiezeng@google.com (Steve Zeng)
     *
     */
    public static class PowerAwareTask implements Callable<MeasurementResult> {

        private MeasurementTask realTask;
        private MeasurementScheduler scheduler;

        public PowerAwareTask(MeasurementTask task,
                              MeasurementScheduler scheduler) {
            realTask = task;
            this.scheduler = scheduler;
        }

        @Override
        public MeasurementResult call() throws MeasurementError {
            MeasurementResult result = null;
            logger.info("Running:\n" + realTask.toString());
            try {
                scheduler.setCurrentTask(realTask);
                try {
                    logger.info("Calling PowerAwareTask " + realTask);
                    result = realTask.call();
                    logger.info("Got result " + result);
                    return result;
                } catch (MeasurementError e) {
                    logger.info("Got MeasurementError running task", e);
                    throw e;
                } catch (Exception e) {
                    logger.error("Got exception running task", e);
                    MeasurementError err = new MeasurementError(e.getMessage(), e);
                    throw err;
                }
            } finally {
                scheduler.setCurrentTask(null);
                logger.info("Done running:\n" + realTask.toString());
            }
        }
    }

    /**
     * Sets the current task being run. In the current implementation, the synchronized keyword is not
     * needed because only one thread runs measurements and calls this method. It is not thread safe.
     */
    public void setCurrentTask(MeasurementTask task) {
        this.currentTask = task;
    }

    private class TaskComparator implements Comparator<MeasurementTask> {
        @Override
        public int compare(MeasurementTask task1, MeasurementTask task2) {
            return task1.compareTo(task2);
        }
    }
}
