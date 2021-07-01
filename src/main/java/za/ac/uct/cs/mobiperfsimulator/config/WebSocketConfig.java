package za.ac.uct.cs.mobiperfsimulator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.client.WebSocketConnectionManager;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import za.ac.uct.cs.mobiperfsimulator.handler.CustomWSHandler;

@Configuration
public class WebSocketConfig {

    @Value("${measurement.server.endpoint}")
    private String webSocketUri;

    @Bean
    public WebSocketConnectionManager wsConnectionManager() {

        //Generates a web socket connection
        WebSocketConnectionManager manager = new WebSocketConnectionManager(
                new StandardWebSocketClient(),
                new CustomWSHandler(),
                this.webSocketUri);

        //Will connect as soon as possible
        manager.setAutoStartup(true);

        return manager;
    }
}
