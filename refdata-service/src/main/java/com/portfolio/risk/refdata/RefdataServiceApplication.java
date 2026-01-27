package com.portfolio.risk.refdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.portfolio.risk")
public class RefdataServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(RefdataServiceApplication.class, args);
    }
}
