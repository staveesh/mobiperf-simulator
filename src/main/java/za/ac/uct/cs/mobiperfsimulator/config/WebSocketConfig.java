package za.ac.uct.cs.mobiperfsimulator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.util.List;

@Configuration
public class WebSocketConfig {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketConfig.class);

    private final StompSessionHandler sessionHandler;

    public WebSocketConfig(StompSessionHandler sessionHandler) {
        this.sessionHandler = sessionHandler;
    }

    @Bean
    @Retryable(value = Exception.class, maxAttempts = 10, backoff = @Backoff(delay = 1000, multiplier = 2, random = true))
    public StompSession initSession(String deviceId, String uri) throws Exception {
        final WebSocketHttpHeaders handshakeHeaders = new WebSocketHttpHeaders();
        StompHeaders connectHeaders = new StompHeaders();
        connectHeaders.add("deviceId", deviceId);
        return stompClient().connect(uri, handshakeHeaders, connectHeaders, sessionHandler).get();
    }

    @Bean
    public WebSocketStompClient stompClient() {
        WebSocketClient simpleWebSocketClient = new StandardWebSocketClient();
        WebSocketStompClient stompClient = new WebSocketStompClient(simpleWebSocketClient);
        stompClient.setMessageConverter(new StringMessageConverter());
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.initialize();
        stompClient.setTaskScheduler(scheduler);
        return stompClient;
    }
}
