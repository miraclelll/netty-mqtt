package com.peachl.nettyserver.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * @Author: zjw
 * @Date: 2021/4/30 14:31
 * @Desc:
 */
public class MqttServerInitializer extends ChannelInitializer {

    private final SslContext sslctx;

    private static final MqttServerHandler HANDLER = new MqttServerHandler();


    public MqttServerInitializer(SslContext sslctx) {
        this.sslctx = sslctx;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
//        pipeline.addLast(new LoggingHandler(LogLevel.INFO));
        pipeline.addLast();
        pipeline.addLast(MqttEncoder.INSTANCE);
        pipeline.addLast(new MqttDecoder(81920));
//        pipeline.addLast("timeout", new IdleStateHandler(30, 0, 20, TimeUnit.SECONDS));
        pipeline.addLast(HANDLER);
        if(null != sslctx) {
            pipeline.addLast(sslctx.newHandler(channel.alloc()));
        }
    }

}
