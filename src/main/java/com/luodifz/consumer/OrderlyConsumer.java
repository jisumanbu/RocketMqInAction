package com.luodifz.consumer;

import com.luodifz.util.JsonUtil;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * Created by liujinjing on 2017/7/19.
 */
public abstract class OrderlyConsumer<T> extends AbstractConsumer implements MessageListenerOrderly {

    private final static Logger logger = LoggerFactory.getLogger(OrderlyConsumer.class);

    public MessageListener getConsumer() {
        return this;
    }

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        MessageExt msg = msgs.get(0);
        logger.info("received message >>>\n{}", msg);

        if (isDuplicateMessage(msg)) {
            return ConsumeOrderlyStatus.SUCCESS;
        }

        Class<T> clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        T objectMsg = JsonUtil.fromJson(new String(msg.getBody()), clazz);

        return doConsumeMessage(objectMsg, context);
    }

    public abstract ConsumeOrderlyStatus doConsumeMessage(T msg, ConsumeOrderlyContext context);
}
