package com.luodifz.producer.callback;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liujinjing on 2017/6/20.
 */
public class DefaultSendCallback implements SendCallback{
    private final static Logger logger = LoggerFactory.getLogger(DefaultSendCallback.class);

    private Message message;

    public DefaultSendCallback(Message message) {
        this.message = message;
    }

    @Override
    public void onSuccess(SendResult sendResult) {
        logger.info("Successfully sent out Message[{}]", message);
    }

    /**
     * TODO, save into DB to support retry logic to make sure send successfully.
     */
    @Override
    public void onException(Throwable e) {
        logger.error("Failed to send Message[{}]", message);
    }
}
