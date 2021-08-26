/* Copyright 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.ac.uct.cs.mobiperfsimulator.measurements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.stomp.StompSession;
import za.ac.uct.cs.mobiperfsimulator.Constants;
import za.ac.uct.cs.mobiperfsimulator.service.BeanUtil;
import za.ac.uct.cs.mobiperfsimulator.service.WebSocketService;
import za.ac.uct.cs.mobiperfsimulator.util.MeasurementJsonConvertor;
import za.ac.uct.cs.mobiperfsimulator.util.Util;

import java.io.*;
import java.net.*;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * A callable that executes a ping task using one of three methods
 */
public class PingTask extends MeasurementTask {

    private static final Logger logger = LoggerFactory.getLogger(PingTask.class);

    private StompSession wsSession;

    // Type name for internal use
    public static final String TYPE = "ping";
    // Human readable name for the task
    public static final String DESCRIPTOR = "ping";
    /* Default payload size of the ICMP packet, plus the 8-byte ICMP header resulting in a total of
     * 64-byte ICMP packet */
    public static final int DEFAULT_PING_PACKET_SIZE = 56;
    public static final int DEFAULT_PING_TIMEOUT = 10;

    private Process pingProc = null;
    private String PING_METHOD_CMD = "ping_cmd";
    private String PING_METHOD_JAVA = "java_ping";
    private String PING_METHOD_HTTP = "http";
    private String targetIp = null;

    // Track data consumption for this task to avoid exceeding user's limit
    private long dataConsumed;

    /**
     * Encode ping specific parameters, along with common parameters inherited from MeasurmentDesc
     *
     * @author wenjiezeng@google.com (Steve Zeng)
     */
    public static class PingDesc extends MeasurementDesc {
        public String pingExe = null;
        // Host address either in the numeric form or domain names
        public String target = null;
        // The payload size in bytes of the ICMP packet
        public int packetSizeByte = PingTask.DEFAULT_PING_PACKET_SIZE;
        public int pingTimeoutSec = PingTask.DEFAULT_PING_TIMEOUT;


        public PingDesc(String key, Date startTime,
                        Date endTime, double intervalSec, long count, long priority,
                        Map<String, String> params, int instanceNumber) throws InvalidParameterException {
            super(PingTask.TYPE, key, startTime, endTime, intervalSec, count,
                    priority, params, instanceNumber);
            initializeParams(params);
            if (this.target == null || this.target.length() == 0) {
                throw new InvalidParameterException("PingTask cannot be created due "
                        + " to null target string");
            }
        }

        @Override
        protected void initializeParams(Map<String, String> params) {
            if (params == null) {
                return;
            }

            this.target = params.get("target");

            try {
                String val = null;
                if ((val = params.get("packet_size_byte")) != null && val.length() > 0 &&
                        Integer.parseInt(val) > 0) {
                    this.packetSizeByte = Integer.parseInt(val);
                }
                if ((val = params.get("ping_timeout_sec")) != null && val.length() > 0 &&
                        Integer.parseInt(val) > 0) {
                    this.pingTimeoutSec = Integer.parseInt(val);
                }
            } catch (NumberFormatException e) {
                throw new InvalidParameterException("PingTask cannot be created due to invalid params");
            }
        }

        @Override
        public String getType() {
            return PingTask.TYPE;
        }
    }

    public static Class getDescClass() throws InvalidClassException {
        return PingDesc.class;
    }

    public PingTask(MeasurementDesc desc) {
        super(new PingDesc(desc.key, desc.startTime, desc.endTime, desc.intervalSec,
                desc.count, desc.priority, desc.parameters, desc.instanceNumber));
        dataConsumed = 0;
    }

    /**
     * Returns a copy of the PingTask
     */
    @Override
    public MeasurementTask clone() {
        MeasurementDesc desc = this.measurementDesc;
        PingDesc newDesc = new PingDesc(desc.key, desc.startTime, desc.endTime,
                desc.intervalSec, desc.count, desc.priority, desc.parameters, desc.instanceNumber);
        return new PingTask(newDesc);
    }

    /* We will use three methods to ping the requested resource in the order of PING_COMMAND,
     * JAVA_ICMP_PING, and HTTP_PING. If all fails, then we declare the resource unreachable */
    @Override
    public MeasurementResult call() throws MeasurementError {
        PingDesc desc = (PingDesc) measurementDesc;
        int ipByteLength;
        try {
            InetAddress addr = InetAddress.getByName(desc.target);
            // Get the address length
            ipByteLength = addr.getAddress().length;
            logger.info("IP address length is " + ipByteLength);
            // All ping methods ping against targetIp rather than desc.target
            targetIp = addr.getHostAddress();
            logger.info("IP is " + targetIp);
        } catch (UnknownHostException e) {
            throw new MeasurementError("Unknown host " + desc.target);
        }
        wsSession = BeanUtil.getBean(StompSession.class);
        try {
            logger.info("running ping command");
            // Prevents the phone from going to low-power mode where WiFi turns off
            MeasurementResult result = executePingCmdTask(ipByteLength);
            logger.debug("Ping Result sending initiated");
            wsSession.send(Constants.STOMP_SERVER_JOB_RESULT_ENDPOINT, MeasurementJsonConvertor.toJsonString(result));
            return result;
        } catch (MeasurementError e) {
            try {
                logger.info("running java ping");
                MeasurementResult result = executeJavaPingTask();
                logger.debug("Java Ping Result sending initiated");
                wsSession.send(Constants.STOMP_SERVER_JOB_RESULT_ENDPOINT, MeasurementJsonConvertor.toJsonString(result));
                return result;
            } catch (MeasurementError ee) {
                logger.info("running http ping");
                MeasurementResult result = executeHttpPingTask();
                logger.debug("http Ping Result sending initiated");
                wsSession.send(Constants.STOMP_SERVER_JOB_RESULT_ENDPOINT, MeasurementJsonConvertor.toJsonString(result));
                return result;
            }
        }
    }

    @Override
    public String getType() {
        return PingTask.TYPE;
    }

    @Override
    public String getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public int getProgress() {
        return this.progress;
    }

    private MeasurementResult constructResult(ArrayList<Double> rrtVals, double packetLoss,
                                              int packetsSent, long expStart, long expEnd, String pingMethod, boolean success) {

        MeasurementResult result = new MeasurementResult(BeanUtil.getBean(WebSocketService.class).getDeviceId(), PingTask.TYPE, System.currentTimeMillis() * 1000,
                success, this.measurementDesc, expEnd - expStart);

        if (success) {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double mdev, avg, filteredAvg;
            double total = 0;

            if (rrtVals.size() == 0) {
                return null;
            }

            for (double rrt : rrtVals) {
                if (rrt < min) {
                    min = rrt;
                }
                if (rrt > max) {
                    max = rrt;
                }
                total += rrt;
            }

            avg = total / rrtVals.size();
            mdev = Util.getStandardDeviation(rrtVals, avg);
            filteredAvg = filterPingResults(rrtVals, avg);

            result.addResult("expStart", expStart);
            result.addResult("expEnd", expEnd);
            result.addResult("time_ms", expEnd - expStart);
            result.addResult("target_ip", targetIp);
            result.addResult("mean_rtt_ms", avg);
            result.addResult("min_rtt_ms", min);
            result.addResult("max_rtt_ms", max);
            result.addResult("stddev_rtt_ms", mdev);
            if (filteredAvg != avg) {
                result.addResult("filtered_mean_rtt_ms", filteredAvg);
            }
            result.addResult("packet_loss", packetLoss);
            result.addResult("packets_sent", packetsSent);
            result.addResult("ping_method", pingMethod);
        }

        logger.info(MeasurementJsonConvertor.toJsonString(result));
        return result;
    }

    private void cleanUp(Process proc) {
        try {
            if (proc != null) {
                proc.destroy();
            }
        } catch (Exception e) {
            logger.warn("Unable to kill ping process" + e.getMessage());
        }
    }

    /* Compute the average of the filtered rtts.
     * The first several ping results are usually extremely large as the device needs to activate
     * the wireless interface and resolve domain names. Such distorted measurements are filtered out
     *
     */
    private double filterPingResults(final ArrayList<Double> rrts, double avg) {
        double rrtAvg = avg;
        // Our # of results should be less than the # of times we ping
        try {
            ArrayList<Double> filteredResults =
                    Util.applyInnerBandFilter(rrts, Double.MIN_VALUE, rrtAvg * Constants.PING_FILTER_THRES);
            // Now we compute the average again based on the filtered results
            if (filteredResults != null && filteredResults.size() > 0) {
                rrtAvg = Util.getSum(filteredResults) / filteredResults.size();
            }
        } catch (InvalidParameterException e) {
            logger.error(e.getMessage());
        }
        return rrtAvg;
    }

    // Runs when SystemState is IDLE
    private MeasurementResult executePingCmdTask(int ipByteLen) throws MeasurementError {
        logger.info("Starting executePingCmdTask");
        PingDesc pingTask = (PingDesc) this.measurementDesc;
        String errorMsg = "";
        MeasurementResult measurementResult = null;
        // TODO(Wenjie): Add a exhaustive list of ping locations for different Android phones
        pingTask.pingExe = Util.pingExecutableBasedOnIPType(ipByteLen);
        logger.info("Ping executable is " + pingTask.pingExe);
        if (pingTask.pingExe == null) {
            logger.error("Ping executable not found");
            throw new MeasurementError("Ping executable not found");
        }
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        try {
            String command = Util.constructCommand(pingTask.pingExe, "-i",
                    Constants.DEFAULT_INTERVAL_BETWEEN_ICMP_PACKET_SEC,
                    "-s", pingTask.packetSizeByte, "-w", pingTask.pingTimeoutSec, "-c",
                    Constants.PING_COUNT_PER_MEASUREMENT, targetIp);
            logger.info("Running: " + command);
            pingProc = Runtime.getRuntime().exec(command);
            dataConsumed += pingTask.packetSizeByte * Constants.PING_COUNT_PER_MEASUREMENT * 2;

            // Grab the output of the process that runs the ping command
            InputStream is = pingProc.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line = null;
            int lineCnt = 0;
            ArrayList<Double> rrts = new ArrayList<Double>();
            ArrayList<Integer> receivedIcmpSeq = new ArrayList<Integer>();
            double packetLoss = Double.MIN_VALUE;
            int packetsSent = Constants.PING_COUNT_PER_MEASUREMENT;
            // Process each line of the ping output and store the rrt in array rrts.
            while ((line = br.readLine()) != null) {
                // Ping prints a number of 'param=value' pairs, among which we only need the
                // 'time=rrt_val' pair
                String[] extractedValues = Util.extractInfoFromPingOutput(line);
                if (extractedValues != null) {
                    int curIcmpSeq = Integer.parseInt(extractedValues[0]);
                    double rrtVal = Double.parseDouble(extractedValues[1]);

                    // ICMP responses from the system ping command could be duplicate and out of order
                    if (!receivedIcmpSeq.contains(curIcmpSeq)) {
                        rrts.add(rrtVal);
                        receivedIcmpSeq.add(curIcmpSeq);
                    }
                }

                this.progress = 100 * ++lineCnt / Constants.PING_COUNT_PER_MEASUREMENT;
                this.progress = Math.min(Constants.MAX_PROGRESS_BAR_VALUE, progress);
                // Get the number of sent/received pings from the ping command output
                int[] packetLossInfo = Util.extractPacketLossInfoFromPingOutput(line);
                if (packetLossInfo != null) {
                    packetsSent = packetLossInfo[0];
                    int packetsReceived = packetLossInfo[1];
                    packetLoss = 1 - ((double) packetsReceived / (double) packetsSent);
                }

                logger.info(line);
            }
            // Use the output from the ping command to compute packet loss. If that's not
            // available, use an estimation.
            if (packetLoss == Double.MIN_VALUE) {
                packetLoss = 1 - ((double) rrts.size() / (double) Constants.PING_COUNT_PER_MEASUREMENT);
            }
            measurementResult = constructResult(rrts, packetLoss, packetsSent, startTime, endTime, PING_METHOD_CMD, true);
        } catch (IOException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        } catch (SecurityException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        } catch (NumberFormatException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        } catch (InvalidParameterException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        } finally {
            // All associated streams with the process will be closed upon destroy()
            cleanUp(pingProc);
        }

        if (measurementResult == null) {
            logger.error("Error running ping: " + errorMsg);
            measurementResult = constructResult(null, 0.0, 0, startTime, System.currentTimeMillis(), PING_METHOD_CMD, false);
        }
        return measurementResult;
    }

    // Runs when the ping command fails
    private MeasurementResult executeJavaPingTask() throws MeasurementError {
        PingDesc pingTask = (PingDesc) this.measurementDesc;
        long pingStartTime = 0;
        long pingEndTime = 0;
        ArrayList<Double> rrts = new ArrayList<Double>();
        String errorMsg = "";
        MeasurementResult result = null;
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        long totalPingDelay = 0;
        try {
            int timeOut = (int) (3000 * (double) pingTask.pingTimeoutSec /
                    Constants.PING_COUNT_PER_MEASUREMENT);
            int successfulPingCnt = 0;
            for (int i = 0; i < Constants.PING_COUNT_PER_MEASUREMENT; i++) {
                pingStartTime = System.currentTimeMillis();
                boolean status = InetAddress.getByName(targetIp).isReachable(timeOut);
                pingEndTime = System.currentTimeMillis();
                if(i == Constants.PING_COUNT_PER_MEASUREMENT-1)
                    endTime = pingEndTime;
                long rrtVal = pingEndTime - pingStartTime;
                if (status) {
                    totalPingDelay += rrtVal;
                    rrts.add((double) rrtVal);
                }
                this.progress = 100 * i / Constants.PING_COUNT_PER_MEASUREMENT;
            }
            logger.info("java ping succeeds");
            double packetLoss = 1 - ((double) rrts.size() / (double) Constants.PING_COUNT_PER_MEASUREMENT);

            dataConsumed += pingTask.packetSizeByte * Constants.PING_COUNT_PER_MEASUREMENT * 2;

            result = constructResult(rrts, packetLoss, Constants.PING_COUNT_PER_MEASUREMENT, startTime, endTime, PING_METHOD_JAVA, true);
        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        } catch (IOException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        }
        if (result == null) {
            logger.info("java ping fails");
            logger.error(errorMsg);
            totalPingDelay = System.currentTimeMillis() - startTime;
            result = constructResult(rrts, 0.0, Constants.PING_COUNT_PER_MEASUREMENT, startTime, endTime, PING_METHOD_JAVA, false);
        }
        return result;
    }

    /**
     * Use the HTTP Head method to emulate ping. The measurement from this method can be
     * substantially (2x) greater than the first two methods and inaccurate. This is because,
     * depending on the implementing of the destination web server, either a quick HTTP
     * response is replied or some actual heavy lifting will be done in preparing the response
     */
    private MeasurementResult executeHttpPingTask() throws MeasurementError {
        long pingStartTime = 0;
        long pingEndTime = 0;
        ArrayList<Double> rrts = new ArrayList<Double>();
        PingDesc pingTask = (PingDesc) this.measurementDesc;
        String errorMsg = "";
        MeasurementResult result = null;
        long startTime = System.currentTimeMillis();
        try {
            long duration = 0;

            URL url = new URL("http://" + pingTask.target);

            int timeOut = (int) (3000 * (double) pingTask.pingTimeoutSec /
                    Constants.PING_COUNT_PER_MEASUREMENT);

            for (int i = 0; i < Constants.PING_COUNT_PER_MEASUREMENT; i++) {
                pingStartTime = System.currentTimeMillis();
                HttpURLConnection httpClient = (HttpURLConnection) url.openConnection();
                httpClient.setRequestProperty("Connection", "close");
                httpClient.setRequestMethod("HEAD");
                httpClient.setReadTimeout(timeOut);
                httpClient.setConnectTimeout(timeOut);
                httpClient.connect();
                pingEndTime = System.currentTimeMillis();
                httpClient.disconnect();
                rrts.add((double) (pingEndTime - pingStartTime));
                duration += (pingEndTime - pingStartTime);
                this.progress = 100 * i / Constants.PING_COUNT_PER_MEASUREMENT;
            }
            logger.info("HTTP get ping succeeds");
            logger.info("RTT is " + rrts.toString());
            double packetLoss = 1 - ((double) rrts.size() / (double) Constants.PING_COUNT_PER_MEASUREMENT);
            result = constructResult(rrts, packetLoss, Constants.PING_COUNT_PER_MEASUREMENT, startTime, pingEndTime, PING_METHOD_HTTP, true);
            dataConsumed += pingTask.packetSizeByte * Constants.PING_COUNT_PER_MEASUREMENT * 2;

        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        } catch (IOException e) {
            logger.error(e.getMessage());
            errorMsg += e.getMessage() + "\n";
        }
        if (result == null) {
            logger.info("java ping fails");
            logger.error(errorMsg);
            result = constructResult(rrts, 0.0, Constants.PING_COUNT_PER_MEASUREMENT, startTime, System.currentTimeMillis(), PING_METHOD_HTTP, false);
        }
        return result;
    }

    @Override
    public String toString() {
        PingDesc desc = (PingDesc) measurementDesc;
        return "[Ping]\n  Target: " + desc.target + "\n  Interval (sec): " + desc.intervalSec
                + "\n  Next run: " + desc.startTime;
    }

    @Override
    public void stop() {
        cleanUp(pingProc);
    }


    /**
     * Data sent so far by this task.
     * <p>
     * We count packets sent directly to calculate the data sent
     */
    @Override
    public long getDataConsumed() {
        return dataConsumed;
    }
}
