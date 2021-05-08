package com.peachl.nettyserver;

import com.peachl.nettyserver.server.MqttServer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import javax.annotation.Resource;

@EnableAsync
@SpringBootApplication
public class NettyServerApplication implements CommandLineRunner {

    @Resource
    private MqttServer mqttServer;

    public static void main(String[] args) {
        SpringApplication.run(NettyServerApplication.class, args);
    }

    @Override
    @Async
    public void run(String... args) throws Exception {
        mqttServer.init();
    }
}
