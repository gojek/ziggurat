package tech.gojek.ziggurat;

import clojure.lang.Keyword;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import tech.gojek.ziggurat.test.Fixtures;

import java.util.List;
import java.util.Properties;

public class ProducerTest {

    @BeforeClass
    public static void setup() {
        Fixtures.mountConfig();
        Fixtures.mountProducer();
    }

    @AfterClass
    public static void teardown() {
        Fixtures.unmountAll();
    }

    @Test
    public void shouldSendStringData() throws InterruptedException {
        String kafkaTopic = "ziggurat-java-test";

        Producer.send(Keyword.intern("default"), kafkaTopic, "Key", "Sent from Java");

        List<KeyValue<String,String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(getStringConsumerConfig(),
                kafkaTopic, 1, 2000);

        Assert.assertEquals("Key", result.get(0).key);
        Assert.assertEquals("Sent from Java", result.get(0).value);
    }

    @Test
    public void shouldSendByteArrayData() throws InterruptedException {
        String kafkaTopic = "ziggurat-java-test-byte-array";
        Producer.send(Keyword.intern("with-byte-array-producer"), kafkaTopic, "Key".getBytes(), "Sent from Java".getBytes());

        List<KeyValue<byte[],byte[]>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(getByteArrayConsumerConfig(),
                kafkaTopic, 1, 2000);

        //Byte arrays converted to strings for comparison
        Assert.assertEquals("Key", new String(result.get(0).key));
        Assert.assertEquals("Sent from Java", new String(result.get(0).value));
    }

    private Properties getByteArrayConsumerConfig() {
        Properties consumerProperties = getComonConsumerProperties();
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return consumerProperties;
    }

    private Properties getStringConsumerConfig() {
        Properties consumerProperties = getComonConsumerProperties();
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerProperties;
    }

    private Properties getComonConsumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ziggurat-consumer");
        return consumerProperties;
    }
}
