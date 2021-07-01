package za.ac.uct.cs.mobiperfsimulator;

public interface Constants {

    String USER_AGENT = "UCT-MobiPerf (Linux)";
    String PING_EXECUTABLE = "ping";
    String PING6_EXECUTABLE = "ping6";

    boolean DEFAULT_START_ON_BOOT = false;
    /** Constants used in various measurement tasks */
    float RESOURCE_UNREACHABLE = Float.MAX_VALUE;
    int PING_COUNT_PER_MEASUREMENT = 10;
    int DEFAULT_DNS_COUNT_PER_MEASUREMENT = 1;

    // Default interval in seconds between system measurements of a given measurement type
    double DEFAULT_SYSTEM_MEASUREMENT_INTERVAL_SEC = 60 * 60;
    // Default interval in seconds between user measurements of a given measurement type
    double DEFAULT_USER_MEASUREMENT_INTERVAL_SEC = 5;
    // Default value for the '-i' option in the ping command
    double DEFAULT_INTERVAL_BETWEEN_ICMP_PACKET_SEC = 0.5;

    float PING_FILTER_THRES = (float) 1.4;
    int MAX_CONCURRENT_PING = 3;
    // Default # of pings per hop for traceroute
    int DEFAULT_PING_CNT_PER_HOP = 3;
    int HTTP_STATUS_OK = 200;
    int THREAD_POOL_SIZE = 1;
    int MAX_TASK_QUEUE_SIZE = 200;
    long MARGIN_TIME_BEFORE_TASK_SCHEDULE = 500;
    long SCHEDULE_POLLING_INTERVAL = 500;
    String INVALID_IP = "";

    /** Constants used in MeasurementScheduler.java */
    // The default checkin interval in seconds
    long DEFAULT_CHECKIN_INTERVAL_SEC = 60 * 60L;
    long MIN_CHECKIN_RETRY_INTERVAL_SEC = 20L;
    long MAX_CHECKIN_RETRY_INTERVAL_SEC = 60L;
    int MAX_CHECKIN_RETRY_COUNT = 3;
    long PAUSE_BETWEEN_CHECKIN_CHANGE_MSEC = 2 * 1000L;
    // default minimum battery percentage to run measurements
    int DEFAULT_BATTERY_THRESH_PRECENT = 10;
    boolean DEFAULT_MEASURE_WHEN_CHARGE = true;
    long MIN_TIME_BETWEEN_MEASUREMENT_ALARM_MSEC = 3 * 1000L;

    /** Constants used in BatteryCapPowerManager.java */
    /** The default battery level if we cannot read it from the system */
    int DEFAULT_BATTERY_LEVEL = 0;
    /** The default maximum battery level if we cannot read it from the system */
    int DEFAULT_BATTERY_SCALE = 100;
    int DEFAULT_BATTERY_TEMPERATURE = 0;
    /** Tasks expire in seven days. Expired tasks will be removed from the scheduler.
     * In general, schedule updates from the server should take care of this automatically. */
    long TASK_EXPIRATION_MSEC = 7 * 24 * 3600 * 1000;


    /** Constants used in MeasurementMonitorActivity.java */
    int MAX_LIST_ITEMS = 128;

    int INVALID_PROGRESS = -1;
    int MAX_PROGRESS_BAR_VALUE = 100;
    // A progress greater than MAX_PROGRESS_BAR_VALUE indicates the end of the measurement
    int MEASUREMENT_END_PROGRESS = MAX_PROGRESS_BAR_VALUE + 1;
    int DEFAULT_USER_MEASUREMENT_COUNT = 1;

    int MAX_USER_MEASUREMENT_COUNT = 10;

    long MIN_CHECKIN_INTERVAL_SEC = 120;

    int DEFAULT_DATA_MONITOR_PERIOD_DAY= 1;

    String STOMP_SERVER_TASKS_ENDPOINT = "/user/%s/queue/jobs";
    String STOMP_SERVER_JOB_RESULT_ENDPOINT = "/device/job-result";
}
