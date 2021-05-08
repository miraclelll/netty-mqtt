package com.peachl.nettyserver.entity;

import io.netty.channel.ChannelHandlerContext;
import lombok.Data;

/**
 * @Author: zjw
 * @Date: 2021/5/7 15:42
 * @Desc:
 */
@Data
public class MqttCtx {

    private String id;

    private String willTopic;

    private ChannelHandlerContext ctx;

}
