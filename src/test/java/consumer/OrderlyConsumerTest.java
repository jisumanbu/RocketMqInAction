package consumer;

import com.luodifz.consumer.ConcurrentlyConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import producer.ProducerTest;

/**
 * Created by liujinjing on 2017/11/21.
 */
public class OrderlyConsumerTest extends ConcurrentlyConsumer<String> {
    private final static Logger logger = LoggerFactory.getLogger(OrderlyConsumerTest.class);

    @Override
    public ConsumeConcurrentlyStatus doConsumeMessage(String msg, ConsumeConcurrentlyContext context) {
        logger.info("get orderly message : " + msg);
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public String getConsumerGroup() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getTopic() {
        return ProducerTest.TOPIC_ORDERLY;
    }

    @Override
    public String getTags() {
        return "sendOrderly";
    }
}
