package com.luodifz.consumer;

import com.luodifz.util.JsonUtil;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * Created by liujinjing on 2017/7/13.
 */
public abstract class ConcurrentlyConsumer<T> extends AbstractConsumer implements MessageListenerConcurrently {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentlyConsumer.class);

    public MessageListener getConsumer() {
        return this;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        MessageExt msg = msgs.get(0);
        logger.info("received message >>>\n{}", msg);

        if (isDuplicateMessage(msg)) {
            logger.warn("Have already been consumed and is not RETRY message, skip it.\n{}", msg);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        Class<T> clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        T objectMsg = JsonUtil.fromJson(new String(msg.getBody()), clazz);

        return doConsumeMessage(objectMsg, context);
    }

    public abstract ConsumeConcurrentlyStatus doConsumeMessage(T msg, ConsumeConcurrentlyContext context);
}
