package info.gtaneja.kafka.reader;

import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ReaderTest {

    public static final String topicPrefix = "test-topic-";
    public static final EmbeddedKafkaConfig embeddedKafkaConfig = EmbeddedKafkaConfig.defaultConfig();
    public static AtomicInteger topicCounter = new AtomicInteger();

    public static String getNextTopic() {
        return String.format("%s%d", topicPrefix, topicCounter.incrementAndGet());
    }

    @BeforeClass
    public static void init() {
        EmbeddedKafka.start(embeddedKafkaConfig);
    }

    @Test
    public void testHappyPath() throws ExecutionException, InterruptedException {
        String topic = getNextTopic();
        Reader reader = Reader.create(getTestConsumerProperties());
        createTopic(reader.getAdmin(), topic, 1);
        List<List<byte[][]>> batches = new ArrayList<>();
        int numBatches = 5;
        for(int batch = 0 ; batch < numBatches; batch ++ ) {
            List<byte[][]> data = IntStream.range(0, 10).mapToObj(i -> new byte[2][2]).collect(Collectors.toList());
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            sendMsg(topicPartition, data);
            batches.add(data);
            // Compare without end offset
            compareResult(batches, reader.read(topicPartition, 0));
            long latestOffset = getLatestOffset(reader.getAdmin(), topicPartition);

            // Compare with end offset
            compareResult(batches, reader.read(topicPartition, 0, latestOffset));
        }
    }

    @Test
    public void testTxn() {

    }

    @Test
    public void testAbortedTxn() {

    }

    @Test
    public void testDeadLetterTxn() {

    }

    private void createTopic(Admin admin,
                             String topic,
                             int numPartitions) throws ExecutionException, InterruptedException {
        CreateTopicsResult result =
                admin.createTopics(List.of(new NewTopic(topic, numPartitions, (short) 1)));
        result.all().get();
    }

    private long getLatestOffset(Admin admin,
                                 TopicPartition topicPartition) throws ExecutionException, InterruptedException {
        return admin.listOffsets(Map.of(topicPartition, OffsetSpec.latest()))
                .partitionResult(topicPartition).get().offset();
    }

    private void sendMsg(TopicPartition topicPartition,
                         List<byte[][]> kvs) {
        Properties properties = getProducerProperties();
        Producer producer = new KafkaProducer<byte[], byte[]>(properties);
        kvs.forEach(kv -> producer.send(new ProducerRecord(topicPartition.topic(),
                topicPartition.partition(), kv[0], kv[1])));
        producer.flush();
    }

    private KafkaConsumer<byte[], byte[]> createConsumer() {
        return null;
    }


    private Properties getProducerProperties() {
        Properties p = new Properties();
        p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("bootstrap.servers", "localhost:" + embeddedKafkaConfig.kafkaPort());
        embeddedKafkaConfig.customProducerProperties().foreach(t -> p.put(t._1, t._2));
        return p;
    }

    private Properties getTestConsumerProperties() {
        Properties p = new Properties();
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("bootstrap.servers", "localhost:" + embeddedKafkaConfig.kafkaPort());
        embeddedKafkaConfig.customProducerProperties().foreach(t -> p.put(t._1, t._2));
        return p;
    }

    private void compareResult(List<List<byte[][]>> expectedBatches, Iterator<Record> result) {
        AtomicInteger i = new AtomicInteger();
        List<byte[][]> items = expectedBatches.stream().flatMap(list -> list.stream()).collect(Collectors.toList());
        result.forEachRemaining(r -> {
            ByteBuffer key = r.key();
            ByteBuffer value = r.value();
            int currentIndex = i.getAndIncrement();
            byte[] expectedKey = items.get(currentIndex)[0];
            byte[] expectedValue = items.get(currentIndex)[1];
            assertEquals(expectedKey.length, key.remaining());
            assertEquals(expectedValue.length, value.remaining());
            byte[] keyBuffer = new byte[key.remaining()];
            byte[] valueBuffer = new byte[value.remaining()];
            key.get(keyBuffer);
            value.get(valueBuffer);
            assertArrayEquals(expectedKey, keyBuffer);
            assertArrayEquals(expectedValue, valueBuffer);
        });
        assertEquals(items.size(), i.get());
    }
}
