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

/**
 * Created by liujinjing on 2017/6/1.
 * <p>
 * <dl>
 *     <dt>主要发送方式
 *         <dd>同步发送、异步发送 -> send、sendAsyn</dd>
 *         <dd>顺序发送（接受者/ consumer也必须是按顺序接收） -> *Orderly</dd>
 *         <dd>延迟发送 -> *Delayed</dd>
 * <p>
 * <dt>sender自动失败重试，如果最终还是发送失败，理应保存数据一面数据丢失
 *      <dd>sender实现了retry，client没有必要retry
 *          <ol>
 *              <li>defaultSendingTimeout = 3000 mills</li>
 *              <li>retryTimesWhenSendFailed = 2</li>
 *          </ol>
 *      </dd>
 *      <dd>如果发送失败，确保消息不丢失（未实现）
 *      <ol>
 *          <li>MqMessageSender会保存消息到DB，同时抛出unCheckedException</li>
 *          <li>应用程序接收到Exception后应该终止运行</li>
 *          <li>检查后重启，可以从DB中重新发送</li>
 *      </ol>
 *      </dd>
 * </dt>
 * <p>
 * <dt>producerGroup
 *      <dd>one producerGroup per application as default
 *      </dd>
 *      <dd>producerGroup用来表示一个发送消息应用
 *      </dd>
 * <dd>
 * 一个 Producer Group 下包含多个 Producer 实例，可以是多台机器，
 * 也可以是一台机器的多个进程，或者一个进程的多个 Producer 对象。
 * </dd>
 * <dd>
 * 一个 Producer Group 可以发送多个 Topic 消息.
 * </dd>
 * <dd>Producer Group 作用如下:
 *      <ul>
 *          <li>标识一类 Producer</li>
 *          <li>可以通过运维工具查询这个发送消息应用下有多个 Producer 实例</li>
 *          <li>发送分布式事务消息时，如果 Producer 中途意外宕机，Broker 会主动回调 Producer Group 内的任意一台机器来确认事务状态。</li>
 *      </ul>
 * </dd>
 * </dt>
 * <p>
 * <dt>consumer Group
 * </dt>
 * <p>
 * <p>
 * </dl>
 */
public class MqMessageSender {

    private final static Logger logger = LoggerFactory.getLogger(MqMessageSender.class);

    private final static DefaultMQProducer producer;

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
            @Override
            public void run() {
                producer.shutdown();
            }
        }));
    }

    public static void shutdown() {
        producer.shutdown();
    }

    /**
     * Send message synchronized.
     * <p>
     * if sendResult.getSendStatus() != SEND_OK || exception -> fail
     * <p>
     * TODO:
     *  1. check maxMessageSize
     *  2. if failed to send message, save message into DB before exit in case lost data
     */
    public static SendResult send(String topic, String tags, String messageKey, String msgContent) throws Exception {
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

    /**
     * @param topic
     * @param tags
     * @param objectMessage -> subClass of AbstractObjectMessage
     * @param sendCallback  -> default : DefaultSendCallback
     */
    public static <T extends AbstractObjectMessage> void sendObjectAsyn(String topic, String tags, T objectMessage, SendCallback sendCallback) {
        String key = objectMessage.getKey();
        String msgContent = JsonUtil.toJson(objectMessage);
        sendAsyn(topic, tags, key, msgContent, sendCallback);
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

    public static SendResult sendOrderly(String topic, String tags, String key, String msgContent) throws Exception {

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
            @Override
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
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object queueIndexSeed) {
                int hashcode = queueIndexSeed.hashCode();
                int queueIndex = hashcode % mqs.size();
                return mqs.get(queueIndex);
            }

        }, tags);

    }

}
