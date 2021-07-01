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
import za.ac.uct.cs.mobiperfsimulator.util.MeasurementJsonConvertor;

import java.util.HashMap;

/**
 * POJO that represents the result of a measurement
 *
 * @see MeasurementDesc
 */
public class MeasurementResult {

    private static final Logger logger = LoggerFactory.getLogger(MeasurementResult.class);

    private String deviceId;
    private long timestamp;
    private boolean success;
    private String taskKey;
    private String type;
    private MeasurementDesc parameters;
    private HashMap<String, String> values;
    private long executionTime;
    /**
     * @param type
     * @param timeStamp
     * @param success
     * @param measurementDesc
     */
    public MeasurementResult(String id, String type,
                             long timeStamp, boolean success, MeasurementDesc measurementDesc, long executionTime) {
        super();
        this.taskKey = measurementDesc.key;
        this.deviceId = id;
        this.type = type;
        this.timestamp = timeStamp;
        this.success = success;
        this.parameters = measurementDesc;
        this.parameters.parameters = null;
        this.values = new HashMap<>();
        this.executionTime = executionTime;
    }

    /* Returns the type of this result */
    public String getType() {
        return parameters.getType();
    }

    /* Add the measurement results of type String into the class */
    public void addResult(String resultType, Object resultVal) {
        this.values.put(resultType, MeasurementJsonConvertor.toJsonString(resultVal));
    }

    /* Returns a string representation of the result */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        try {
            switch (type) {
                case PingTask.TYPE:
                    getPingResult(values);
                    break;
                case HttpTask.TYPE:
                    getHttpResult(values);
                    break;
                case DnsLookupTask.TYPE:
                    getDnsResult(values);
                    break;
                case TracerouteTask.TYPE:
                    getTracerouteResult(values);
                    break;
                case TCPThroughputTask.TYPE:
                    getTCPThroughputResult(values);
                    break;
                default:
                    logger.error("Failed to get results for unknown measurement type " + type);
                    break;
            }
            return builder.toString();
        } catch (NumberFormatException e) {
            logger.error("Exception occurs during constructing result string for user", e);
        } catch (ClassCastException e) {
            logger.error("Exception occurs during constructing result string for user", e);
        } catch (Exception e) {
            logger.error("Exception occurs during constructing result string for user", e);
        }
        return "Measurement has failed";
    }

    private void getPingResult(HashMap<String, String> values) {
        PingTask.PingDesc desc = (PingTask.PingDesc) parameters;
        logger.info("[Ping]");
        logger.info("Target: " + desc.target);
        String ipAddress = removeQuotes(values.get("target_ip"));
        // TODO: internationalize 'Unknown'.
        if (ipAddress == null) {
            ipAddress = "Unknown";
        }
        logger.info("IP address: " + ipAddress);

        if (success) {
            float packetLoss = Float.parseFloat(values.get("packet_loss"));
            int count = Integer.parseInt(values.get("packets_sent"));
            logger.info("\n" + count + " packets transmitted, " + (int) (count * (1 - packetLoss)) +
                    " received, " + (packetLoss * 100) + "% packet loss");

            float value = Float.parseFloat(values.get("mean_rtt_ms"));
            logger.info("Mean RTT: " + String.format("%.1f", value) + " ms");

            value = Float.parseFloat(values.get("min_rtt_ms"));
            logger.info("Min RTT:  " + String.format("%.1f", value) + " ms");

            value = Float.parseFloat(values.get("max_rtt_ms"));
            logger.info("Max RTT:  " + String.format("%.1f", value) + " ms");

            value = Float.parseFloat(values.get("stddev_rtt_ms"));
            logger.info("Std dev:  " + String.format("%.1f", value) + " ms");
        } else {
            logger.info("Failed");
        }
    }

    private void getHttpResult(HashMap<String, String> values) {
        HttpTask.HttpDesc desc = (HttpTask.HttpDesc) parameters;
        logger.info("[HTTP]");
        logger.info("URL: " + desc.url);

        if (success) {
            int headerLen = Integer.parseInt(values.get("headers_len"));
            int bodyLen = Integer.parseInt(values.get("body_len"));
            int time = Integer.parseInt(values.get("time_ms"));
            logger.info("");
            logger.info("Downloaded " + (headerLen + bodyLen) + " bytes in " + time + " ms");
            logger.info("Bandwidth: " + (headerLen + bodyLen) * 8 / time + " Kbps");
        } else {
            logger.info("Download failed, status code " + values.get("code"));
        }
    }

    private void getDnsResult(HashMap<String, String> values) {
        DnsLookupTask.DnsLookupDesc desc = (DnsLookupTask.DnsLookupDesc) parameters;
        logger.info("[DNS Lookup]");
        logger.info("Target: " + desc.target);

        if (success) {
            String ipAddress = removeQuotes(values.get("address"));
            if (ipAddress == null) {
                ipAddress = "Unknown";
            }
            logger.info("\nAddress: " + ipAddress);
            int time = Integer.parseInt(values.get("timeMs"));
            logger.info("Lookup time: " + time + " ms");
        } else {
            logger.info("Failed");
        }
    }

    private void getTracerouteResult(HashMap<String, String> values) {
        TracerouteTask.TracerouteDesc desc = (TracerouteTask.TracerouteDesc) parameters;
        logger.info("[Traceroute]");
        logger.info("Target: " + desc.target);

        if (success) {
            // Manually inject a new line
            logger.info(" ");

            int hops = Integer.parseInt(values.get("num_hops"));
            int hop_str_len = String.valueOf(hops + 1).length();
            for (int i = 0; i < hops; i++) {
                String key = "hop_" + i + "_addr_1";
                String ipAddress = removeQuotes(values.get(key));
                if (ipAddress == null) {
                    ipAddress = "Unknown";
                }
                String hop_str = String.valueOf(i + 1);
                String hopInfo = hop_str;
                for (int j = 0; j < hop_str_len + 1 - hop_str.length(); ++j) {
                    hopInfo += " ";
                }
                hopInfo += ipAddress;
                // Maximum IP address length is 15.
                for (int j = 0; j < 16 - ipAddress.length(); ++j) {
                    hopInfo += " ";
                }

                key = "hop_" + i + "_rtt_ms";
                // The first and last character of this string are double quotes.
                String timeStr = removeQuotes(values.get(key));
                if (timeStr == null) {
                    timeStr = "Unknown";
                }

                float time = Float.parseFloat(timeStr);
                logger.info(hopInfo + String.format("%6.2f", time) + " ms");
            }
        } else {
            logger.info("Failed");
        }
    }

    private void getTCPThroughputResult(HashMap<String, String> values) {
        TCPThroughputTask.TCPThroughputDesc desc = (TCPThroughputTask.TCPThroughputDesc) parameters;
        if (desc.dir_up) {
            logger.info("[TCP Uplink]");
        } else {
            logger.info("[TCP Downlink]");
        }
        logger.info("Target: " + desc.target);

        if (success) {
            logger.info("");
            // Display result with precision up to 2 digit
            String speedInJSON = values.get("tcp_speed_results");
            String dataLimitExceedInJSON = values.get("data_limit_exceeded");
            String displayResult = "";

            double tp = desc.calMedianSpeedFromTCPThroughputOutput(speedInJSON);
            double KB = Math.pow(2, 10);
            if (tp < 0) {
                displayResult = "No results available.";
            } else if (tp > KB * KB) {
                displayResult = "Speed: " + String.format("%.2f", tp / (KB * KB)) + " Gbps";
            } else if (tp > KB) {
                displayResult = "Speed: " + String.format("%.2f", tp / KB) + " Mbps";
            } else {
                displayResult = "Speed: " + String.format("%.2f", tp) + " Kbps";
            }

            // Append notice for exceeding data limit
            if (dataLimitExceedInJSON.equals("true")) {
                displayResult += "\n* Task finishes earlier due to exceeding " +
                        "maximum number of " + ((desc.dir_up) ? "transmitted" : "received") +
                        " bytes";
            }
            logger.info(displayResult);
        } else {
            logger.info("Failed");
        }
    }

    /**
     * Removes the quotes surrounding the string. If |str| is null, returns null.
     */
    private String removeQuotes(String str) {
        return str != null ? str.replaceAll("^\"|\"", "") : null;
    }
}
