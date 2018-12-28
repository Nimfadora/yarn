package com.vasileva.container.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.yarn.client.YarnClient;

@EnableAutoConfiguration
public class ClientApp {

    public static void main(String[] args) {
        SpringApplication.run(ClientApp.class, args)
                .getBean(YarnClient.class)
                .submitApplication();
    }

}