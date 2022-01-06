package info.gtaneja.kafka.reader;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.CompletedFetch;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;

/**
 * Reader allows to read messages from any to any offset.
 * All the implementation are blocking.
 * Readers are thread safe and they can be used by multiple threads simultaneously.
 */
public interface Reader {

    /**
     * Create a Reader Object
     * @param properties The consumer configuration properties
     * @return Reader implementation.
     */
    static Reader create(Properties properties) {
        return new DefaultReader(properties);
    }

    private static ClientResponse sendFetch(ConsumerNetworkClient consumerNetworkClient,
                                            Node node,
                                            TopicPartition topicPartition,
                                            Long startFetchOffset,
                                            int maxSize) {
        java.util.Map<TopicPartition, FetchRequest.PartitionData> fetchData =
                Collections.singletonMap(topicPartition, new FetchRequest.PartitionData(startFetchOffset,
                        startFetchOffset, maxSize, Optional.empty()));
        FetchRequest.Builder fetchRequest = FetchRequest.Builder.forConsumer(Integer.MAX_VALUE, 0,
                fetchData).metadata(FetchMetadata.INITIAL);
        RequestFuture<ClientResponse> requestFuture = consumerNetworkClient.send(node, fetchRequest);
        consumerNetworkClient.poll(requestFuture);
        if (requestFuture.failed()) {
            throw new RuntimeException(requestFuture.exception());
        }
        return requestFuture.value();
    }


    /**
     * Retrieves the next batch from the brokers. Current implementation try retrieves that data
     * from the nodes till it successfully fetches
     * @param consumerNetworkClient
     * @param topicPartition
     * @param startFetchOffset start offset inclusive.
     * @param fetchMaxBytes Maximum size for which data will be retrieved.
     * @param isrs List of in-sync replica hosts
     * @return BatchResult. Client should be able to update isrs based on BatchResult.
     */
    static BatchResult getNextBatch(ConsumerNetworkClient consumerNetworkClient,
                                            TopicPartition topicPartition,
                                            long startFetchOffset,
                                            int fetchMaxBytes,
                                            List<Node> isrs) {
        if(isrs.isEmpty()) {
            throw new RuntimeException("isr list is empty");
        }
        ClientResponse response = null;
        Node successNode = null;
        Map<Node, Exception> exceptions = new HashMap<>();
        for(Node isr : isrs) {
            try {
                response = Reader.sendFetch(consumerNetworkClient, isr, topicPartition,
                        startFetchOffset,
                        fetchMaxBytes);
                successNode = isr;
                break;
            } catch (Exception e ) {
                exceptions.put(isr, e);
            }
        }
        if (response == null) {
            throw new RuntimeException("Filed to fetch from nodes " +
                    Arrays.toString(exceptions
                            .entrySet()
                            .stream()
                            .map( e -> e.getKey().toString() + e.getValue().getMessage())
                            .toArray()));
        }

        FetchResponse.PartitionData<Records> partitionData =
                ((FetchResponse<Records>) response.responseBody())
                        .responseData().get(topicPartition);

        CompletedFetch fetch = new CompletedFetch(
                topicPartition,
                partitionData,
                partitionData.records().batches().iterator(),
                startFetchOffset,
                response.requestHeader().apiVersion(),
                IsolationLevel.READ_COMMITTED, false,
                null);
        return new BatchResult(fetch.fetchRecords(Integer.MAX_VALUE), successNode);
    }

    /**
     *
     * @param keySerializer
     * @param valueSerializer
     * @param topic
     * @param partition
     * @param record
     * @param <K>
     * @param <V>
     * @return
     */
    static <K, V> ConsumerRecord<K, V> recordToConsumerRecord(Deserializer<K> keySerializer,
                                                               Deserializer<V> valueSerializer,
                                                               String topic,
                                                               int partition,
                                                               Record record) {
        K k = null;
        V v = null;
        byte[] key = null, value = null;
        int serializedKeySize = NULL_SIZE;
        int serializedValueSize = NULL_SIZE;
        long checksum ;
        if(record.checksumOrNull() ==null) {
            checksum = NULL_CHECKSUM;
        } else {
            checksum = record.checksumOrNull();
        }

        if(record.key() != null) {
            serializedKeySize = record.key().remaining();
            key = new byte[serializedKeySize];
            record.key().get(key);
        }

        if(record.value() != null) {
            serializedValueSize = record.key().remaining();
            value = new byte[serializedValueSize];
            record.value().get(value);
        }

        v = valueSerializer.deserialize(topic, value);
        k = keySerializer.deserialize(topic, key);

        return new ConsumerRecord<K, V>(topic,
                partition,
                record.offset(),
                record.timestamp(),
                TimestampType.NO_TIMESTAMP_TYPE,
                checksum,
                serializedKeySize, serializedValueSize,
                k,
                v);
    }

    /**
     * Read messages from a partition.
     * @param topicPartition Topic and partition.
     * @param startOffset start offsets inclusive.
     * @return Iterator of Records. It only contains committed messages.
     */
    Iterator<Record> read(TopicPartition topicPartition, long startOffset);

    /**
     * Read messages from a partition.
     * @param topicPartition Topic and partition.
     * @param startOffset start offset inclusive.
     * @param endOffset end offset exclusive.
     * @return Iterator of Records. It only contains committed messages.
     */
    Iterator<Record> read(TopicPartition topicPartition, long startOffset, long endOffset);

    /**
     * Read messages from a partition.
     * @param topicPartition Topic and partition.
     * @param startOffset start offset inclusive.
     * @param isrs In-sync replicas of the partition.
     *            It makes sure if any in sync replica is not available then retry the operation with next node
     * @return Iterator of Records. It only contains committed messages from startOffset to latest Offset.
     */
    Iterator<Record> read(TopicPartition topicPartition, long startOffset, List<Node> isrs);

    /**
     * Read messages from a partition.
     * @param topicPartition Topic and partition.
     * @param startOffset start offset inclusive.
     * @param endOffset end offset exclusive.
     * @param isrs In-sync replicas of the partition.
     *             It makes sure if any in sync replica is not available then retry the operation with next node
     * @return Iterator of Records. It only contains committed messages.
     */
    Iterator<Record> read(TopicPartition topicPartition, long startOffset, long endOffset, List<Node> isrs);

    /**
     *
     * @return Admin client used by this Reader interface.
     */
    Admin getAdmin();


    public class BatchResult {

        public final List<Record> batch;
        public final Node source;
        BatchResult(List<Record> batch, Node source){
            this.batch = batch;
            this.source = source;
        }
    }

    class DefaultReader implements Reader {

        private final ConsumerNetworkClient consumerNetworkClient;
        private final AdminClient adminClient;
        private final int maxSize;

        DefaultReader(Properties properties) {
            this(new KafkaConsumer(properties), AdminClient.create(properties), 1024);
        }

        DefaultReader(KafkaConsumer<byte[], byte[]> consumer,
                      AdminClient adminClient,
                      int maxSize) {
            this.adminClient = adminClient;
            try {
                Field f  = KafkaConsumer.class.getDeclaredField("client");
                f.setAccessible(true);
                consumerNetworkClient = (ConsumerNetworkClient) f.get(consumer);
                this.maxSize = maxSize;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Admin getAdmin() {
            return adminClient;
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition, long startOffset) {
            List<Node> isrs = getIsrs(topicPartition);
            return read(topicPartition, startOffset, isrs);
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition, long startOffset, long endOffset) {
            List<Node> isrs = getIsrs(topicPartition);
            return read(topicPartition, startOffset, endOffset, isrs);
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition, long startOffset, List<Node> isrs) {
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(Map.of(
                    topicPartition, OffsetSpec.latest()));
            try {
                long endOffset = listOffsetsResult.all().get()
                        .get(topicPartition).offset();
                return read(topicPartition, startOffset, endOffset);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition,
                                     long startOffset,
                                     long endOffset,
                                     List<Node> isrs) {
            if(startOffset < 0 || endOffset < 0 ) {

            }
            return new KafkaIterator(consumerNetworkClient,
                    topicPartition,
                    isrs,
                    startOffset, endOffset, maxSize);
        }

        private List<Node> getIsrs(TopicPartition topicPartition) {
            DescribeTopicsResult describeTopicsResult =
                    adminClient.describeTopics(List.of(topicPartition.topic()));
            try {
                TopicDescription description =
                        describeTopicsResult.values().get(topicPartition.topic()).get();
                if(description == null) {
                    return Collections.emptyList();
                }
                List<TopicPartitionInfo> infos = description
                        .partitions()
                        .stream()
                        .filter(topicPartitionInfo -> topicPartitionInfo.partition() == topicPartition.partition())
                        .collect(Collectors.toList());
                if(infos.size() > 0 ) {
                    return infos.get(0).isr();
                } else {
                    return Collections.emptyList();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}


