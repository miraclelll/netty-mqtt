package com.peachl.nettyserver.entity;

import lombok.Data;

/**
 * @Author: zjw
 * @Date: 2021/5/7 14:46
 * @Desc:
 */
@Data
public class MqttClient {

    private String clientId;

    private String clientName;

    private String clientPwd;

    private String willTopic;

    private byte[] willMsg;

}
