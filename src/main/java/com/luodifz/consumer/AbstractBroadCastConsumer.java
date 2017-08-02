package com.luodifz.consumer;

import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * Created by liujinjing on 2017/6/19.
 */
public abstract class AbstractBroadCastConsumer extends AbstractConsumer {
    public MessageModel getMessageModel() {
        return MessageModel.BROADCASTING;
    }
}
