package info.gtaneja.kafka.reader;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;

import java.lang.reflect.Field;
import java.util.*;

public interface Reader {

    Iterator<Record> read(TopicPartition topicPartition, long startOffset);
    Iterator<Reader> read(TopicPartition topicPartition, long startOffset, long endOffset);
    Iterator<Record> read(TopicPartition topicPartition, long startOffset, Collection<Node> isr);
    Iterator<Record> read(TopicPartition topicPartition, long startOffset, long endOffset, Collection<Node> isr);

    public static Reader with(KafkaConsumer<byte[], byte[]> consumer, AdminClient adminClient,
                              int maxSize) {
        return new DefaultReader(consumer, adminClient, maxSize);
    }

    static class DefaultReader implements Reader {

        private ConsumerNetworkClient consumerNetworkClient;
        private KafkaConsumer<byte[], byte[]> consumer ;
        private AdminClient adminClient;
        private int maxSize;

        DefaultReader(KafkaConsumer<byte[], byte[]> consumer, AdminClient adminClient, int maxSize ) {
            Field f = null;
            this.consumer = consumer;
            this.adminClient = adminClient;
            try {
                f = KafkaConsumer.class.getDeclaredField("client");
                f.setAccessible(true);
                consumerNetworkClient = (ConsumerNetworkClient) f.get(consumer);
                this.maxSize = maxSize;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition, long startOffset) {
            return null;
        }

        @Override
        public Iterator<Reader> read(TopicPartition topicPartition, long startOffset, long endOffset) {
            return null;
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition, long startOffset, Collection<Node> isr) {
            return null;
        }

        @Override
        public Iterator<Record> read(TopicPartition topicPartition, long startOffset, long endOffset, Collection<Node> isr) {
            return null;
        }
    }

    public static ClientResponse sendFetch( ConsumerNetworkClient consumerNetworkClient,
                                            Node node,
                                            TopicPartition topicPartition,
                                            Long startFetchOffset,
                                            int maxSize)  {
        java.util.Map<TopicPartition, FetchRequest.PartitionData> fetchData =
        Collections.singletonMap(topicPartition, new FetchRequest.PartitionData(startFetchOffset,
                startFetchOffset, maxSize, Optional.empty()));
        FetchRequest.Builder fetchRequest = FetchRequest.Builder.forConsumer(Integer.MAX_VALUE, 0,
                fetchData).metadata(FetchMetadata.INITIAL);

        RequestFuture<ClientResponse> requestFuture = consumerNetworkClient.send(node, fetchRequest);
        consumerNetworkClient.poll(requestFuture);
        if(requestFuture.failed()) {
            throw new RuntimeException(requestFuture.exception());
        }
        return requestFuture.value();
    }
}


