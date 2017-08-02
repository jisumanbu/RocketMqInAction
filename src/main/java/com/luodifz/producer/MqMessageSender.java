package com.luodifz.producer;

import com.luodifz.producer.callback.DefaultSendCallback;
import com.luodifz.util.JsonUtil;
import com.luodifz.vo.AbstractObjectMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

public class MqMessageSender {
    private final static Logger logger = LoggerFactory.getLogger(MqMessageSender.class);

    public final static DefaultMQProducer producer;

    static {
        //FIXME, take projectName as producerGroup
        producer = new DefaultMQProducer("groupProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.setInstanceName(UUID.randomUUID().toString());
        try {
            producer.start();
        } catch (MQClientException e) {
            logger.error("MQ sender failed to startup with ERROR : " + e.getMessage(), e);
            throw new RuntimeException(e);
        }

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                producer.shutdown();
            }
        }));
    }

    public static void shutdown() {
        producer.shutdown();
    }

    public static <T extends AbstractObjectMessage> SendResult sendObjectMsg(String topic, String tags, T objectMessage) throws
            Exception {
        String messageKey = objectMessage.getKey();
        String msgContent = JsonUtil.toJson(objectMessage);
        return sendMsg(topic, tags, messageKey, msgContent);
    }

    /**
     * Send message synchronized.
     *
     * if sendResult.getSendStatus() != SEND_OK || exception -> fail
     *
     * TODO, maxMessageSize
     *
     */
    public static SendResult sendMsg(String topic, String tags, String messageKey, String msgContent) throws Exception {
        SendResult sendResult = null;

        try {
            Message msg = new Message(topic,
                    tags,
                    messageKey,
                    msgContent.getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            sendResult = producer.send(msg);
        } catch (Exception e) {
            //TODO, save message into DB before exit
            throw new Exception(e);// RunTimeException ?
        }

        return sendResult;
    }

    /**
     * @param topic
     * @param tags
     * @param objectMessage -> subClass of AbstractObjectMessage
     * @param sendCallback  -> default : DefaultSendCallback
     */
    public static <T extends AbstractObjectMessage> void sendObjectMsgAsyn(String topic, String tags, T objectMessage, SendCallback sendCallback) {
        String key = objectMessage.getKey();
        String msgContent = JsonUtil.toJson(objectMessage);
        sendAsyn(topic, tags, key, msgContent, sendCallback);
    }


    //have not finished designing yet, so do not use it now.

    /**
     * How to deal success results and exceptions
     *
     * @param topic
     * @param tags
     * @param key
     * @param msgContent
     * @param sendCallback
     * @return
     */
    public static void sendAsyn(String topic, String tags, String key, String msgContent, SendCallback sendCallback) {
        sendAsynDelayed(topic, tags, key, msgContent, sendCallback, 0);
    }

    public static void sendAsynDelayed(String topic, String tags, String key, String msgContent, SendCallback sendCallback, int delayTimeLevel) {

        try {
            Message msg = new Message(topic,
                    tags,
                    key,
                    msgContent.getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            if (delayTimeLevel != 0) {
                msg.setDelayTimeLevel(delayTimeLevel);
            }

            if (sendCallback == null) {
                sendCallback = new DefaultSendCallback(msg);
            }

            producer.send(msg, sendCallback);

        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        } catch (RemotingException e) {
            logger.error(e.getMessage(), e);
        } catch (MQClientException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static SendResult sendMsgOrderly(String topic, String tags, String key, String msgContent) throws Exception {

        Message msg = new Message(topic,
                tags,
                key,
                msgContent.getBytes(RemotingHelper.DEFAULT_CHARSET)
        );

        logger.info("Send message orderly : {}", msg);
        return producer.send(msg, new MessageQueueSelector() {
            /**
             * select queueId by tags
             *  1. same tags, should be always in same queue
             *  2. different tags can be in same queue
             */
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object queueIndexSeed) {
                int hashcode = queueIndexSeed.hashCode();
                int queueIndex = hashcode % mqs.size();
                return mqs.get(queueIndex);
            }

        }, tags);

    }

    public static SendResult sendOrderlyDelayed(String topic, String tags, String key, String msgContent, int delayTimeLevel) throws Exception {

        Message msg = new Message(topic,
                tags,
                key,
                msgContent.getBytes(RemotingHelper.DEFAULT_CHARSET)
        );

        msg.setDelayTimeLevel(delayTimeLevel);

        logger.info("Send message orderly : {}", msg);
        return producer.send(msg, new MessageQueueSelector() {
            /**
             * select queueId by tags
             *  1. same tags, should be always in same queue
             *  2. different tags can be in same queue
             */
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object queueIndexSeed) {
                int hashcode = queueIndexSeed.hashCode();
                int queueIndex = hashcode % mqs.size();
                return mqs.get(queueIndex);
            }

        }, tags);

    }

}
