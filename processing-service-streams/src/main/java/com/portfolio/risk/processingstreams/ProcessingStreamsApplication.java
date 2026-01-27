package com.portfolio.risk.processingstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.portfolio.risk")
public class ProcessingStreamsApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProcessingStreamsApplication.class, args);
    }
}
