package za.ac.uct.cs.mobiperfsimulator.service;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.stereotype.Service;
import za.ac.uct.cs.mobiperfsimulator.config.WebSocketConfig;
import za.ac.uct.cs.mobiperfsimulator.measurements.MeasurementTask;
import za.ac.uct.cs.mobiperfsimulator.orchestration.MeasurementScheduler;
import za.ac.uct.cs.mobiperfsimulator.util.MeasurementJsonConvertor;

import java.util.Vector;

@Service
public class WebSocketService {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketService.class);

    private String webSocketUri;

    private String deviceId;

    private String speedTestServer;

    private StompSession stompSession;
    private WebSocketConfig webSocketConfig;
    @Autowired
    private MeasurementScheduler scheduler;


    @Autowired
    public WebSocketService(WebSocketConfig webSocketConfig,
                            @Value("${measurement.server.endpoint}") String webSocketUri,
                            @Value("${mobiperf.device.id}") String deviceId,
                            @Value("${speed.test.server}") String speedTestServer) {
        this.webSocketConfig = webSocketConfig;
        this.webSocketUri = webSocketUri;
        this.deviceId = deviceId;
        this.speedTestServer = speedTestServer;
        initSession();
    }

    public void initSession() {
        logger.info("Initializing websocket session");
        try {
            this.stompSession = this.webSocketConfig.initSession(deviceId, webSocketUri);
        } catch (Exception e) {
            logger.info("Create stomp session failed ", e);
            this.stompSession = null;
        }
    }

    public void handleMessage(String result) {
        Vector<MeasurementTask> tasksFromServer = new Vector<>();
        JSONArray jsonArray = null;
        try {
            jsonArray = new JSONArray(result);
            for (int i = 0; i < jsonArray.length(); i++) {
                logger.debug("Parsing index " + i);
                JSONObject json = jsonArray.optJSONObject(i);
                logger.debug("Value is " + json);
                if (json != null && MeasurementTask.getMeasurementTypes().contains(json.get("type"))) {
                    try {
                        MeasurementTask task = MeasurementJsonConvertor.makeMeasurementTaskFromJson(json);
                        logger.info(MeasurementJsonConvertor
                                .toJsonString(task.measurementDesc));
                        tasksFromServer.add(task);
                    } catch (IllegalArgumentException e) {
                        logger.warn("Could not create task from JSON: " + e);
                    }
                }
            }
        } catch (JSONException e) {
            logger.error("Invalid JSON received from server");
        }
        scheduler.updateSchedule(tasksFromServer, false);
        scheduler.handleMeasurement();
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getSpeedTestServer() {
        return speedTestServer;
    }
}
