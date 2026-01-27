package com.portfolio.risk.eventgen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.portfolio.risk")
public class EventGeneratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventGeneratorApplication.class, args);
    }
}
