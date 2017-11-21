package com.luodifz.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * Created by liujinjing on 2017/6/1.
 * <p>
 * Concrete consumer listener must provide group/topic, tags
 */
public abstract class AbstractConsumer {

    private final static Logger logger = LoggerFactory.getLogger(AbstractConsumer.class);

    public DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();

    {
        consumer.setConsumerGroup(getConsumerGroup());
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setInstanceName(getInstanceName());

        /*
         *  Note, ConsumeFromWhere
         *      CONSUME_FROM_LAST_OFFSET  -> 默认策略，从该队列最尾开始消费，即跳过历史消息
         *      CONSUME_FROM_FIRST_OFFSET -> 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
         *      CONSUME_FROM_TIMESTAMP    -> 从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.setMessageModel(getMessageModel());
        try {
            consumer.subscribe(getTopic(), getTags());
        } catch (MQClientException e) {
            logger.error(e.getMessage(), e);
        }

        consumer.setMessageListener(getConsumer());

        try {
            consumer.start();
        } catch (MQClientException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.shutdown();
            }
        }));
    }

    private String getInstanceName() {
        return getConsumerGroup() + "_" + getTopic() + "_" + getTags() + "_" + UUID.randomUUID().toString();
    }

    /**
     * insert key into Redis, if already exists then we can tell that this message is received by other consumer instance.
     *
     * @param messageKey
     * @return
     */
    public static boolean isAlreadyConsumed(String topic, String tags, String messageKey) {
        String hashKeyOfRedis = topic + "_" + tags;

        boolean isAlreadyConsumed = false;
        try {
            //FIXME 注意：FedisClient为未实现单例，用户得自行修复以上代码。
            isAlreadyConsumed = !FedisClient.getClient().getSetCache(hashKeyOfRedis).add(messageKey, 1L, TimeUnit.DAYS);
        } catch (Exception e) {
            //if any exception, take default value: isAlreadyConsumed = false;
            logger.error("Met [{}] when check one message if isAlreadyConsumed, we have a risk consuming duplicate message ....", e.getMessage());
        }
        return isAlreadyConsumed;
    }

    public MessageModel getMessageModel() {
        return MessageModel.CLUSTERING;
    }

    public abstract MessageListener getConsumer();

    public abstract String getConsumerGroup();

    public abstract String getTopic();

    public abstract String getTags();

    //if message is from %RETRY% topic -> re-consume
    //if isn't from %RETRY% topic, skip duplicate message.
    public boolean isDuplicateMessage(MessageExt msg) {
        return notFromRetryTopic(msg) && AbstractConsumer.isAlreadyConsumed(msg.getTopic(), msg.getTags(), msg.getKeys());
    }

    private boolean notFromRetryTopic(MessageExt msg) {
        if (msg.getProperties().get("REAL_TOPIC") == null) {
            return true;
        } else {
            boolean notFromRetryTopic = !msg.getProperties().get("REAL_TOPIC").startsWith("%RETRY%");
            if (!notFromRetryTopic) {
                logger.info("Msg[topic:{}, tags:{}, keys:{}] comes from retry topic.", msg.getTopic(), msg.getTags(), msg.getKeys());
            }
            return notFromRetryTopic;
        }
    }
}