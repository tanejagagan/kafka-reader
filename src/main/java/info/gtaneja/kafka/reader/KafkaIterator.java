package info.gtaneja.kafka.reader;

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class KafkaIterator implements Iterator<Record> {

    private final ConsumerNetworkClient consumerNetworkClient;
    private final List<Node> isrs;
    private final TopicPartition topicPartition;
    private final int fetchMaxBytes;
    private final long endOffset;
    private final long startOffset;
    private long nextOffset;
    private Iterator<Record> currentPartitionRecords;
    private boolean finished = false;
    private boolean gotNext = false;
    private Record nextValue;


    public KafkaIterator(ConsumerNetworkClient consumerNetworkClient,
                         TopicPartition topicPartition,
                         List<Node> isrs,
                         long startOffset,
                         long endOffset,
                         int fetchMaxBytes) {
        if(isrs.isEmpty()) {
            throw new RuntimeException("isr list is empty");
        }
        if( endOffset < startOffset) {
            String msg = String.format("fromOffset should be smaller than until offset. fromOffset %d. untilOffset %d",
                    startOffset, endOffset);
            throw new RuntimeException(msg);
        }
        this.consumerNetworkClient = consumerNetworkClient;
        this.isrs = isrs;
        this.topicPartition = topicPartition;
        this.fetchMaxBytes = fetchMaxBytes;
        this.endOffset = endOffset;
        this.startOffset = startOffset;
        this.nextOffset = startOffset;
    }

    private Record getNext() {
        if (nextOffset >= endOffset) {
            // Processed all offsets in this partition.
            finished = true;
            return null;
        } else {
            Record r = readNext();
            if (r == null) {
                if (nextOffset <= startOffset) {
                    // Losing some data. Skip the rest offsets in this partition.
                    throw new RuntimeException("lost data " + topicPartition);
                }
                finished = true;
                return null;
            } else {
                nextOffset = r.offset() + 1;

                return r;
            }
        }
    }

    private Record readNext() {
        if (currentPartitionRecords == null) {
            currentPartitionRecords = getNextBatch(nextOffset, isrs).iterator();
        }
        if (!currentPartitionRecords.hasNext()) {
            currentPartitionRecords = getNextBatch(nextOffset, isrs).iterator();
            if (currentPartitionRecords.hasNext()) {
                return currentPartitionRecords.next();
            } else {
                return null;
            }
        } else {
            return currentPartitionRecords.next();
        }
    }

    private List<Record> getNextBatch(long requestOffset, List<Node> isrs) {
        return Reader.getNextBatch(consumerNetworkClient, topicPartition, requestOffset, fetchMaxBytes, isrs);
    }


    @Override
    public boolean hasNext() {
        if (!finished) {
            if (!gotNext) {
                nextValue = getNext();
                if (finished) {
                    closeIfNeeded();
                }
                gotNext = true;
            }
        }
        return !finished;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException("End of stream");
        }
        gotNext = false;
        return nextValue;
    }

    private void closeIfNeeded() {

    }
}
