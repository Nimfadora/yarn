package com.vasileva.container;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
public class ContainerApp {

    public static void main(String[] args) {
        SpringApplication.run(ContainerApp.class, args);
    }

    @Bean
    public HelloPojo helloPojo() {
        return new HelloPojo();
    }

}
