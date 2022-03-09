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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * A Callable task that handles Traceroute measurements
 */
public class TracerouteTask extends MeasurementTask {

  private StompSession wsSession;

  private static final Logger logger = LoggerFactory.getLogger(TracerouteTask.class);

  // Type name for internal use
  public static final String TYPE = "traceroute";
  // Human readable name for the task
  public static final String DESCRIPTOR = "traceroute";
  /* Default payload size of the ICMP packet, plus the 8-byte ICMP header resulting in a total of 
   * 64-byte ICMP packet */
  public static final int DEFAULT_PING_PACKET_SIZE = 56;
  public static final int DEFAULT_PING_TIMEOUT = 10;
  public static final int DEFAULT_MAX_HOP_CNT = 30;
  // Used to compute progress for user
  public static final int EXPECTED_HOP_CNT = 20;
  public static final int DEFAULT_PINGS_PER_HOP = 3;
  
  private Process pingProc = null;
  private boolean stopRequested = false;
  
  // Track data consumption for this task to avoid exceeding user's limit
  private long dataConsumed;

  /**
   * The description of the Traceroute measurement 
   */
  public static class TracerouteDesc extends MeasurementDesc {
    // the host name or IP address to use as the target of the traceroute.
    public String target;
    // the packet per ICMP ping in the unit of bytes
    private int packetSizeByte;
    // the number of seconds we wait for a ping response.
    private int pingTimeoutSec;
    // the interval between successive pings in seconds
    private double pingIntervalSec; 
    // the number of pings we use for each ttl value
    private int pingsPerHop;        
    // the total number of pings will send before we declarethe traceroute fails
    private int maxHopCount;        
    // the location of the ping binary. Only used internally
    private String pingExe;         
    
    public TracerouteDesc(String key, Date startTime,
                          Date endTime, double intervalSec, long count, long priority, 
                          Map<String, String> params, int instanceNumber, Date addedToQueueAt, Date dispatchTime) throws InvalidParameterException {
      super(TracerouteTask.TYPE, key, startTime, endTime, intervalSec, count, 
          priority, params, instanceNumber, addedToQueueAt, dispatchTime);
      initializeParams(params);
      
      if (target == null || target.length() == 0) {
        throw new InvalidParameterException("Target of traceroute cannot be null");
      }
    }

    @Override
    public String getType() {
      return TracerouteTask.TYPE;
    }
    
    @Override
    protected void initializeParams(Map<String, String> params) {
      
      if (params == null) {
        return;
      }
      
      // HTTP specific parameters according to the design document
      this.target = params.get("target");
      try {        
        String val;
        if ((val = params.get("packet_size_byte")) != null && val.length() > 0 &&
            Integer.parseInt(val) > 0) {
          this.packetSizeByte = Integer.parseInt(val);  
        } else {
          this.packetSizeByte = TracerouteTask.DEFAULT_PING_PACKET_SIZE;
        }
        if ((val = params.get("ping_timeout_sec")) != null && val.length() > 0 &&
            Integer.parseInt(val) > 0) {
          this.pingTimeoutSec = Integer.parseInt(val);
        } else {
          this.pingTimeoutSec = TracerouteTask.DEFAULT_PING_TIMEOUT;
        }
        if ((val = params.get("ping_interval_sec")) != null && val.length() > 0 &&
            Integer.parseInt(val) > 0) {
          this.pingIntervalSec = Integer.parseInt(val);
        } else {
          this.pingIntervalSec = Constants.DEFAULT_INTERVAL_BETWEEN_ICMP_PACKET_SEC;
        }
        if ((val = params.get("pings_per_hop")) != null && val.length() > 0 &&
            Integer.parseInt(val) > 0) {
          this.pingsPerHop = Integer.parseInt(val);
        } else {
          this.pingsPerHop = TracerouteTask.DEFAULT_PINGS_PER_HOP;
        }
        if ((val = params.get("max_hop_count")) != null && val.length() > 0 && 
            Integer.parseInt(val) > 0) {
          this.maxHopCount = Integer.parseInt(val);  
        } else {
          this.maxHopCount = TracerouteTask.DEFAULT_MAX_HOP_CNT;
        }
      } catch (NumberFormatException e) {
        throw new InvalidParameterException("PingTask cannot be created due to invalid params");
      }
    }
    
  }
  
  public TracerouteTask(MeasurementDesc desc) {
    super(new TracerouteDesc(desc.key, desc.startTime, desc.endTime, desc.intervalSec,
      desc.count, desc.priority, desc.parameters, desc.instanceNumber, desc.addedToQueueAt, desc.dispatchTime));
    dataConsumed = 0;
  }
  
  /**
   * Returns a copy of the TracerouteTask
   */
  @Override
  public MeasurementTask clone() {
    MeasurementDesc desc = this.measurementDesc;
    TracerouteDesc newDesc = new TracerouteDesc(desc.key, desc.startTime, desc.endTime, 
      desc.intervalSec, desc.count, desc.priority, desc.parameters, desc.instanceNumber, desc.addedToQueueAt, desc.dispatchTime);
    return new TracerouteTask(newDesc);
  }

  @Override
  public MeasurementResult call() throws MeasurementError {
    
    TracerouteDesc task = (TracerouteDesc) this.measurementDesc;
    int maxHopCount = task.maxHopCount;
    int ttl = 1;
    String hostIp = null;
    String target = task.target;
    boolean success = false;
    ArrayList<HopInfo> hopHosts = new ArrayList<HopInfo>();
    wsSession = BeanUtil.getBean(StompSession.class);
    logger.debug("Starting traceroute on host " + task.target);
    
    try {
      InetAddress hostInetAddr = InetAddress.getByName(target);
      hostIp = hostInetAddr.getHostAddress();
      // add support for ipv6
      int ipByteLen = hostInetAddr.getAddress().length;
      logger.info("IP address length is " + ipByteLen);
      logger.info("IP is " + hostIp);
      task.pingExe = Util.pingExecutableBasedOnIPType(ipByteLen);
      logger.info("Ping executable is " + task.pingExe);
      if (task.pingExe == null) {
        logger.error("Ping Executable not found");
        throw new MeasurementError("Ping Executable not found");
      }
    } catch (UnknownHostException e) {
      logger.error("Cannont resolve host " + target);
      throw new MeasurementError("target " + target + " cannot be resolved");
    }
    MeasurementResult result = null;
    long duration = 0;
    long startTime = System.currentTimeMillis();
    long endTime = 0;
    while (maxHopCount-- >= 0 && !stopRequested) {
      /* Current traceroute implementation sends out three ICMP probes per TTL.
       * One ping every 0.2s is the lower bound before some platforms requires
       * root to run ping. We ping once every time to get a rough rtt as we cannot
       * get the exact rtt from the output of the ping command with ttl being set
       * */
      String command = Util.constructCommand(task.pingExe, "-n", "-t", ttl,
        "-s", task.packetSizeByte, "-c 1", target);
      try {
        double rtt = 0;
        HashSet<String> hostsAtThisDistance = new HashSet<String>();
        int effectiveTask = 0;
        for (int i = 0; i < task.pingsPerHop; i++) {
          pingProc = Runtime.getRuntime().exec(command);
          
          // Actual packet is 28 bytes larger than the size specified.
          // Three packets are sent in each direction
          dataConsumed += (task.packetSizeByte + 28) * 2 * 3;
          
          // Wait for process to finish
          // Enforce thread timeout if pingProc doesn't respond
          ProcWrapper procwrapper = new ProcWrapper(pingProc);
          procwrapper.start();
          try {
            long pingThreadTimeout = 5000;
            procwrapper.join(pingThreadTimeout);
            if (procwrapper.exitStatus == null)
              throw new TimeoutException();
          } catch(InterruptedException ex) {
            procwrapper.interrupt();
            Thread.currentThread().interrupt();
            logger.error("Traceroute process gets interrupted");
            cleanUp(pingProc);
            continue;
          } catch (TimeoutException e) {
            logger.error("Traceroute process timeout");
            cleanUp(pingProc);
            continue;
          }
          rtt += procwrapper.duration;
          duration += procwrapper.duration;
          effectiveTask++;
          
          // Grab the output of the process that runs the ping command
          InputStream is = pingProc.getInputStream();
          BufferedReader br = new BufferedReader(new InputStreamReader(is));
          /* Process each line of the ping output and extracts the intermediate hops into 
           * hostAtThisDistance */ 
          processPingOutput(br, hostsAtThisDistance, hostIp);
          cleanUp(pingProc);
          try {
            Thread.sleep((long) (task.pingIntervalSec * 1000));
          } catch (InterruptedException e) {
            logger.info("Sleep interrupted between ping intervals");
          }
        }
        rtt = (effectiveTask != 0) ? (rtt / effectiveTask) : -1;
        if (rtt == -1) {
          String Unreachablehost = "";
          for (int i = 0; i < task.pingsPerHop; i++) {
            Unreachablehost += "* ";
          }
          hostsAtThisDistance.add(Unreachablehost);
        }
        
        logger.info("RTT is " + rtt);
        hopHosts.add(new HopInfo(hostsAtThisDistance, rtt));

        // Process the extracted IPs of intermediate hops
        StringBuffer progressStr = new StringBuffer(ttl + ": ");
        for (String ip : hostsAtThisDistance) {
          endTime = System.currentTimeMillis();
          // If we have reached the final destination hostIp, print it out and clean up
          if (ip.compareTo(hostIp) == 0) {
            logger.info(ttl + ": " + hostIp);
            logger.info(" Finished! " + target + " reached in " + ttl + " hops");
            
            success = true;
            result = new MeasurementResult(BeanUtil.getBean(WebSocketService.class).getDeviceId(), TracerouteTask.TYPE,
                System.currentTimeMillis() * 1000, success, this.measurementDesc, duration);
            result.addResult("num_hops", ttl);
            result.addResult("time_ms", duration);
            for (int i = 0; i < hopHosts.size(); i++) {
              HopInfo hopInfo = hopHosts.get(i);
              int hostIdx = 1;
              for (String host : hopInfo.hosts) {
                result.addResult("hop_" + i + "_addr_" + hostIdx++, host);
              }
              result.addResult("hop_" + i + "_rtt_ms", String.format("%.3f", hopInfo.rtt));
            }
            result.addResult("expStart", startTime);
            result.addResult("expEnd", endTime);
            String jsonResultString= MeasurementJsonConvertor.toJsonString(result);
            logger.info(jsonResultString);
            wsSession.send(Constants.STOMP_SERVER_JOB_RESULT_ENDPOINT, jsonResultString);
            return result;
          } else {
            // Otherwise, we aggregate various hosts at a given hop distance for printout
            progressStr.append(ip + " | ");
          }
        }
        // Remove the trailing separators
        progressStr.delete(progressStr.length() - 3, progressStr.length());
        logger.info(progressStr.toString());

      } catch (SecurityException e) {
        logger.error("Does not have the permission to run ping on this device");
      } catch (IOException e) {
        logger.error("The ping program cannot be executed");
        logger.error(e.getMessage());
      } finally {
        cleanUp(pingProc);
      }
      ttl++;
      this.progress = (int) (100 * ttl / (double) TracerouteTask.EXPECTED_HOP_CNT);
      this.progress = Math.min(Constants.MAX_PROGRESS_BAR_VALUE, progress);
    }

    duration = System.currentTimeMillis() - startTime;
    result = new MeasurementResult(BeanUtil.getBean(WebSocketService.class).getDeviceId(), TracerouteTask.TYPE,
            System.currentTimeMillis() * 1000, success, this.measurementDesc, duration);
    String jsonResultString=MeasurementJsonConvertor.toJsonString(result);
    logger.info(jsonResultString);
    wsSession.send(Constants.STOMP_SERVER_JOB_RESULT_ENDPOINT, jsonResultString);
    return result;
  }

  public static Class getDescClass() throws InvalidClassException {
    return TracerouteDesc.class;
  }
  
  @Override
  public String getType() {
    return TracerouteTask.TYPE;
  }
  
  @Override
  public String getDescriptor() {
    return DESCRIPTOR;
  }
  
  private void cleanUp(Process proc) {
    if (proc != null) {
      // destroy() closes all open streams
      proc.destroy();
    }
  }

  private void processPingOutput(BufferedReader br, HashSet<String> hostsAtThisDistance,
      String hostIp) throws IOException {
    String line = null;
    while ((line = br.readLine()) != null) {
      logger.debug(line);
      if (line.startsWith("From")) {
        String ip = getHostIp(line);
        if (ip != null && ip.compareTo(hostIp) != 0) {
          hostsAtThisDistance.add(ip);
        }
      } else if (line.contains("time=")) {
        hostsAtThisDistance.add(hostIp);
      }
    }
  }

  /* TODO(Wenjie): The current search for valid IPs assumes the IP string is not a proper 
   * substring of the space-separated tokens. For more robust searching in case different 
   * outputs from ping due to its different versions, we need to refine the search 
   * by testing weather any substring of the tokens contains a valid IP */
  private String getHostIp(String line) {      
    String[] tokens = line.split(" ");
    // In most cases, the second element in the array is the IP
    String tempIp = tokens[1];
    if (isValidIpv4Addr(tempIp) || isValidIpv6Addr(tempIp)) {
      return tempIp;
    } else {
      for (int i = 0; i < tokens.length; i++) {
        if (i == 1) {
          // Examined already
          continue;
        } else {
          if (isValidIpv4Addr(tokens[i]) || isValidIpv6Addr(tokens[i])) {
            return tokens[i];
          }
        }
      }
    }
    
    return null;
  }
  
  // Tells whether the string is an valid IPv4 address
  private boolean isValidIpv4Addr(String ip) {
    String[] tokens = ip.split("\\.");
    if (tokens.length == 4) {
      for (int i = 0; i < 4; i++) {
        try {
          int val = Integer.parseInt(tokens[i]); 
          if (val < 0 || val > 255) {
            return false;
          }
        } catch (NumberFormatException e) {
          logger.debug(ip + " is not a valid IPv4 address");
          return false;
        }
      }
      return true;
    }
    return false;
  }  
  
  //Tells whether the string is an valid IPv6 address
  private boolean isValidIpv6Addr(String ip) {
    int max = Integer.valueOf("FFFF", 16);
    String[] tokens = ip.split("\\:");
    if (tokens.length <= 8) {
      for (int i = 0; i < tokens.length; i++) {
        try {
          // zeros might get grouped
          if (tokens[i].isEmpty())
            continue;
          int val = Integer.parseInt(tokens[i], 16); 
          if (val < 0 || val > max) {
            return false;
          }
        } catch (NumberFormatException e) {
          logger.debug(ip + " is not a valid IPv6 address");
          return false;
        }
      }
      return true;
    }
    return false;
  }
 
  private class HopInfo {
    // The hosts at a given hop distance
    public HashSet<String> hosts;
    // The average RRT for this hop distance
    public double rtt;
    
    protected HopInfo(HashSet<String> hosts, double rtt) {
      this.hosts = hosts;
      this.rtt = rtt;
    }
  }
  
  @Override
  public String toString() {
    TracerouteDesc desc = (TracerouteDesc) measurementDesc;
    return "[Traceroute]\n  Target: " + desc.target + "\n  Interval (sec): " + desc.intervalSec 
    + "\n  Next run: " + desc.startTime;
  }
  
  @Override
  public void stop() {
    stopRequested = true;
    cleanUp(pingProc);
  }
  
  // Measure the actual ping process execution time
  private class ProcWrapper extends Thread {
    public long duration = 0;
    private final Process process;
    private Integer exitStatus = null;
    private ProcWrapper(Process process) {
      this.process = process;
    }
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        exitStatus = process.waitFor();
        duration = System.currentTimeMillis() - startTime;
      } catch (InterruptedException e) {
        logger.error("Traceroute thread gets interrupted");
      }
    }  
  }

  /**
   * Based on counting the number of pings sent
   */
  @Override
  public long getDataConsumed() {
    return dataConsumed;
  }
}
