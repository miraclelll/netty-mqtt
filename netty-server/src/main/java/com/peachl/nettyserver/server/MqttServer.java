package com.peachl.nettyserver.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

/**
 * @Author: zjw
 * @Date: 2021/4/30 14:07
 * @Desc:
 */
@Component
@Slf4j
public class MqttServer {

    public static final Boolean isSSL = System.getProperty("ssl") != null;

    @Value("${mqtt.port}")
    private Integer port;


    public void init() throws CertificateException, SSLException {
        final SslContext sslctx;

        log.info("mqtt服务端启动");

        EventLoopGroup boosGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();

        if(isSSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslctx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslctx = null;
        }

        try {
            ServerBootstrap serverBootstrap = bootstrap.group(boosGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_RCVBUF, 1024*1024*1024)
                    .childHandler(new MqttServerInitializer(sslctx));

            ChannelFuture future = serverBootstrap.bind(port).sync();
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.info("程序中断");
        } finally {
            log.info("释放资源，连接线程数为{}", MqttServerHandler.connNum);
            if(null != boosGroup) {
                boosGroup.shutdownGracefully();
            }
            if(null != workerGroup) {
                workerGroup.shutdownGracefully();
            }
        }
    }

}
