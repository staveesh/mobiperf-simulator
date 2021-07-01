package za.ac.uct.cs.mobiperfsimulator.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.stereotype.Component;
import za.ac.uct.cs.mobiperfsimulator.Constants;
import za.ac.uct.cs.mobiperfsimulator.service.WebSocketService;

import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class StompSessionHandlerImpl implements StompSessionHandler {

    private static final Logger logger = LoggerFactory.getLogger(StompSessionHandlerImpl.class);

    private WebSocketService webSocketService;
    private AtomicBoolean isConnected;

    public StompSessionHandlerImpl() {
        this.isConnected = new AtomicBoolean();
    }

    @Autowired
    @Lazy
    public void setWebSocketService(WebSocketService webSocketService) {
        this.webSocketService = webSocketService;
    }

    @Override
    public void afterConnected(StompSession stompSession, StompHeaders stompHeaders) {
        this.isConnected.set(true);
        stompSession.subscribe(String.format(Constants.STOMP_SERVER_TASKS_ENDPOINT, webSocketService.getDeviceId()), this);
    }

    @Override
    public void handleException(StompSession stompSession, StompCommand stompCommand, StompHeaders stompHeaders, byte[] bytes, Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void handleTransportError(StompSession stompSession, Throwable throwable) {
        if (this.isConnected.get()) {
            this.isConnected.set(false);
            webSocketService.initSession();
        }
    }

    @Override
    public Type getPayloadType(StompHeaders stompHeaders) {
        return String.class;
    }

    @Override
    public void handleFrame(StompHeaders stompHeaders, Object payload) {
        if (payload == null) return;
        logger.info("Received: " + payload);
        webSocketService.handleMessage(payload.toString());
    }
}
