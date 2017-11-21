package consumer;

import com.luodifz.consumer.ConcurrentlyConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;

/**
 * Created by liujinjing on 2017/11/21.
 */
public class ConcurrentlyConsumerTest extends ConcurrentlyConsumer<String>{
    @Override
    public ConsumeConcurrentlyStatus doConsumeMessage(String msg, ConsumeConcurrentlyContext context) {
        return null;
    }

    @Override
    public String getConsumerGroup() {
        return null;
    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public String getTags() {
        return null;
    }
}
