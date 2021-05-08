package com.peachl.nettyserver.server;

import cn.hutool.Hutool;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.ReferenceUtil;
import cn.hutool.core.util.StrUtil;
import com.peachl.nettyserver.entity.MqttClient;
import com.peachl.nettyserver.entity.MqttCtx;
import com.peachl.nettyserver.utils.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;

/**
 * @Author: zjw
 * @Date: 2021/5/6 9:24
 * @Desc: CONNECT：客户端连接到MQTT代理
 * CONNACK：连接确认
 * PUBLISH：新发布消息
 * PUBACK：新发布消息确认，是QoS 1给PUBLISH消息的回复
 * PUBREC：QoS 2消息流的第一部分，表示消息发布已记录
 * PUBREL：QoS 2消息流的第二部分，表示消息发布已释放
 * PUBCOMP：QoS 2消息流的第三部分，表示消息发布完成
 * SUBSCRIBE：客户端订阅某个主题
 * SUBACK：对于SUBSCRIBE消息的确认
 * UNSUBSCRIBE：客户端终止订阅的消息
 * UNSUBACK：对于UNSUBSCRIBE消息的确认
 * PINGREQ：心跳
 * PINGRESP：确认心跳
 * DISCONNECT：客户端终止连接前优雅地通知MQTT代理
 */
@Slf4j
@ChannelHandler.Sharable
public class MqttServerHandler extends ChannelInboundHandlerAdapter {

    public static ConcurrentHashMap<String, MqttClient> clientMap = new ConcurrentHashMap<String, MqttClient>(131072);

    public static ConcurrentHashMap<String, MqttCtx> subMap = new ConcurrentHashMap<String, MqttCtx>(131072);

    /**
     * 新设备连接初始化方法
     * @param channelHandlerContext
     */
    public void channelActive(ChannelHandlerContext channelHandlerContext) {
        log.info("新设备连接，当前时间{}", DateUtil.now());
    }

    /**
     * 设备通讯接口
     * @param ctx
     * @param request
     * @throws Exception
     */
    public void channelRead(ChannelHandlerContext ctx, Object request) throws Exception {
        MqttMessage mqttMsg = (MqttMessage) request;
        if (mqttMsg.decoderResult().isSuccess()) {
            switch (mqttMsg.fixedHeader().messageType()) {
                case CONNECT:
                    //建立连接
                    log.info("建立连接");
                    doConnect(ctx, request);
                    break;
                case CONNACK:
                    //确认连接
                    log.info("确认连接");
                    break;
                case PUBLISH:
                    //新发布消息
                    log.info("新发布消息");
                    doPublish(ctx, request);
                    break;
                case PUBACK:
                    //确认新发布消息
                    log.info("确认新发布消息");
                    break;
                case PUBREC:
                    //QOS2 已记录
                    log.info("已记录");
                    break;
                case PUBREL:
                    //QOS2 已释放
                    log.info("已释放");
                    break;
                case PUBCOMP:
                    //QOS2 已结束
                    log.info("已结束");
                    break;
                case SUBSCRIBE:
                    //订阅
                    log.info("订阅");
                    doSubScribe(ctx, request);
                    break;
                case SUBACK:
                    //确认订阅
                    log.info("确认订阅");
                    break;
                case UNSUBSCRIBE:
                    //终止订阅
                    log.info("终止订阅");
                    doUnSubScribe(ctx, request);
                    break;
                case UNSUBACK:
                    //确认终止订阅消息
                    log.info("确认终止订阅消息");
                    break;
                case PINGREQ:
                    //心跳消息
                    log.info("心跳消息");
                    doPingReq(ctx, request);
                    break;
                case PINGRESP:
                    //心跳确认
                    log.info("心跳确认");
                    break;
                case DISCONNECT:
                    //停止连接
                    log.info("停止连接");
                    doDisConnect(ctx, request);
                    break;
                case AUTH:
                    //增强认证
                    log.info("增强认证");
                    break;
                default:
                    break;
            }
            ReferenceCountUtil.release(mqttMsg);
        }
    }

    /**
     * 断开连接
     * @param ctx
     * @param request
     */
    private void doDisConnect(ChannelHandlerContext ctx, Object request) {
        ChannelId channelId = ctx.pipeline().channel().id();
        if(null != clientMap.get(channelId)) {
            clientMap.remove(channelId);
        }
        ctx.close();
    }

    /**
     * 向订阅目标发送消息方法
     * @param willMsg
     * @param topicName
     * @param messageId
     * @return
     */
    public void buildPublish(byte[] willMsg, String topicName, Integer messageId) {

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, willMsg.length);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, messageId);
        ByteBuf payload = Unpooled.wrappedBuffer(willMsg);
        MqttPublishMessage msg = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
        //遍历订阅主题的缓存列表，将消息发送至所有的订阅列表
        subMap.forEach((k, v) -> {
            if(topicName.equals(v.getWillTopic())) {
                msg.retain();
                v.getCtx().writeAndFlush(msg);
                log.info("发送正常{}", v.getCtx());
            }
        });
        ReferenceCountUtil.release(msg);
    }

    /**
     * 取消mqtt订阅
     * @param ctx
     * @param request
     */
    private void doUnSubScribe(ChannelHandlerContext ctx, Object request) {
        MqttUnsubscribeMessage message = (MqttUnsubscribeMessage) request;
        String channelId = String.valueOf(ctx.pipeline().channel().id());

        //遍历mqtt取消订阅列表，清除相关订阅列表缓存
        AtomicInteger topicNum = new AtomicInteger(message.payload().topics().size());
        message.payload().topics().forEach(topic -> {
            subMap.forEach((k, v) -> {
                if(topicNum.get() > 0 && topic.equals(v.getWillTopic()) && channelId.equals(v.getId())) {
                    subMap.remove(k);
                    topicNum.getAndDecrement();
                }
            });
        });

        MqttMessageIdVariableHeader messageIdVariableHeader = message.variableHeader();
        //  构建返回报文  可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        //  构建返回报文  固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        //  构建返回报文  取消订阅确认
        MqttUnsubAckMessage unSubAck = new MqttUnsubAckMessage(mqttFixedHeaderBack,variableHeaderBack);
        ctx.writeAndFlush(unSubAck);

    }

    /**
     * 异常处理调用遗嘱方法
     * @param ctx
     * @param cause
     * @throws Exception
     */
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        //网络异常发送遗嘱
        if(cause instanceof IOException) {
            String channelId = String.valueOf(ctx.pipeline().channel().id());
            //找到订阅的主题和订阅消息
            MqttClient mqttClient = clientMap.get(channelId);
            //调用服务端发布消息的方法循环向相同订阅的客户端发送消息
            subMap.forEach((k, v) -> {
                if(v.getWillTopic().equals(mqttClient.getWillTopic())) {
                    buildPublish(mqttClient.getWillMsg(), mqttClient.getWillTopic(), -1);
                    //清除对应的订阅缓存
                    subMap.remove(k);
                }
            });
            //清除对应的连接缓存
            clientMap.remove(channelId);
        } else {
            cause.printStackTrace();
        }
        //关闭channel
        if(channel.isActive()) {
            ctx.close();
        }
    }

    /**
     * 订阅调用方法
     * @param ctx
     * @param request
     */
    private void doSubScribe(ChannelHandlerContext ctx, Object request) {
        log.info("开始订阅");
        MqttSubscribeMessage message = (MqttSubscribeMessage) request;
        int messageId = message.variableHeader().messageId();
        if(-1 == messageId) {
            messageId = 1;
        }

        log.info("订阅服务编号为{}", ctx.pipeline().channel().id());

        //获取所有的订阅主题列表
        Set<String> topics = message.payload().topicSubscriptions().stream()
                .map(mqttTopicSubscription -> mqttTopicSubscription.topicName()).collect(Collectors.toSet());

        //循环列表存入缓存
        topics.forEach(topic -> {
            MqttCtx mqttCtx = new MqttCtx();
            mqttCtx.setId(String.valueOf(ctx.pipeline().channel().id()));
            mqttCtx.setCtx(ctx);
            mqttCtx.setWillTopic(topic);
            subMap.put(RandomUtil.randomString(32), mqttCtx);
        });

        //回复ack
        MqttMessageIdVariableHeader from = MqttMessageIdVariableHeader.from(messageId);
        MqttSubAckPayload payload = new MqttSubAckPayload(0);
        MqttSubAckMessage mqttSubAckMessage = new MqttSubAckMessage(Constants.SUBACK_HEADER, from, payload);
        ctx.writeAndFlush(mqttSubAckMessage);
    }

    /**
     * 响应mqtt心跳
     * @param ctx
     * @param request
     */
    private void doPingReq(ChannelHandlerContext ctx, Object request) {
        MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.PINGREQ
                , Boolean.FALSE, MqttQoS.AT_LEAST_ONCE, Boolean.FALSE, 0);
        MqttMessage message = new MqttMessage(header);
        ctx.writeAndFlush(message);
    }

    /**
     * 接受客户端上报消息
     * @param ctx
     * @param request
     */
    private void doPublish(ChannelHandlerContext ctx, Object request) {
        MqttPublishMessage message = (MqttPublishMessage)request;
        ByteBuf payload = message.payload();
        String result = new String(ByteBufUtil.getBytes(payload));
        log.info("终端上报信息为{}", result);

        int packetId = message.variableHeader().packetId();

        buildPublish(ByteBufUtil.getBytes(payload), message.variableHeader().topicName(), packetId);
        if(message.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);
            MqttPubAckMessage puback = new MqttPubAckMessage(Constants.PUBACK_HEADER, variableHeader);
            ctx.writeAndFlush(puback);
        }
    }

    /**
     * 建立mqtt连接
     * @param ctx
     * @param request
     */
    public void doConnect(ChannelHandlerContext ctx, Object request) {
        MqttConnectMessage message = (MqttConnectMessage)request;

        //建立连接对象并存入缓存
        MqttClient mqttClient = new MqttClient();
        mqttClient.setClientId(message.payload().clientIdentifier());
        mqttClient.setClientName(message.payload().userName());
        mqttClient.setClientPwd(new String(message.payload().passwordInBytes(), CharsetUtil.UTF_8));
        mqttClient.setWillTopic(message.payload().willTopic());
        mqttClient.setWillMsg(message.payload().willMessageInBytes());
        clientMap.put(String.valueOf(ctx.pipeline().channel().id()), mqttClient);

        log.info("新设备连接设备编号为：{}",ctx.pipeline().channel().id());

        MqttConnAckVariableHeader variableheader = new MqttConnAckVariableHeader(CONNECTION_ACCEPTED, false);

        String errorMsg = "";
        switch (variableheader.connectReturnCode()) {
            case CONNECTION_ACCEPTED:
                MqttConnAckMessage connAckMessage = new MqttConnAckMessage(Constants.CONNACK_HEADER, variableheader);
                ctx.writeAndFlush(connAckMessage);
                return;
            case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
                errorMsg = "用户名密码错误";
                return;
            case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
                errorMsg = "clientId不允许链接";
                return;
            case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
                errorMsg = "服务不可用";
                return;
            case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
                errorMsg = "mqtt 版本不可用";
                return;
            case CONNECTION_REFUSED_NOT_AUTHORIZED:
                errorMsg = "未授权登录";
                return;
            default:
                errorMsg = "未知错误";
                break;
        }
    }

}
