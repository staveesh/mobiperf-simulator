package za.ac.uct.cs.mobiperfsimulator.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

public class CustomWSHandler implements WebSocketHandler {

    private Logger logger = LoggerFactory.getLogger(CustomWSHandler.class);

    @Override
    public void afterConnectionEstablished(WebSocketSession webSocketSession) throws Exception {
        logger.info("Connected.");
    }

    @Override
    public void handleMessage(WebSocketSession webSocketSession, WebSocketMessage<?> webSocketMessage) throws Exception {

    }

    @Override
    public void handleTransportError(WebSocketSession webSocketSession, Throwable throwable) throws Exception {

    }

    @Override
    public void afterConnectionClosed(WebSocketSession webSocketSession, CloseStatus closeStatus) throws Exception {
        logger.info("Disconnected.");
    }

    @Override
    public boolean supportsPartialMessages() {
        return false;
    }
}
