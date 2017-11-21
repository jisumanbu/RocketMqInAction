package producer;

import com.luodifz.producer.MqMessageSender;
import org.junit.Test;

/**
 * Created by liujinjing on 2017/11/21.
 */
public class ProducerTest {
    public final static String TOPIC_SYN = "topic_syn";
    public final static String TOPIC_ASYN = "topic_Asyn";
    public final static String TOPIC_ORDERLY = "topic_orderly";

    @Test
    public void send_synchronized() throws Exception {
        MqMessageSender.send(TOPIC_SYN, "send", getMessageKey(), "msgContent");
    }

    @Test
    public void send_asynchronous() {
        MqMessageSender.sendAsyn(TOPIC_ASYN, "sendAsyn", getMessageKey(),"msgContent", null);
    }

    @Test
    public void send_orderly() throws Exception {
        for (int i = 0; i < 100; i++) {
            MqMessageSender.sendOrderly(TOPIC_ORDERLY, "sendOrderly", i + "", i + "");
        }

    }

    private String getMessageKey() {
        return System.currentTimeMillis() + "";
    }
}
