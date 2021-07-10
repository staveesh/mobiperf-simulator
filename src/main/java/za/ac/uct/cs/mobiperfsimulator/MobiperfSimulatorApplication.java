package za.ac.uct.cs.mobiperfsimulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MobiperfSimulatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(MobiperfSimulatorApplication.class, args);
	}

}
