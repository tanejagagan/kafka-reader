package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 *  This class is copied from org.apache.kafka.clients.consumer.internals.Fetcher
 *  Need to have a watch on any change in this file. Copied from kafka branch 2.8
 */
public class CompletedFetch {
    private final TopicPartition partition;
    private final Iterator<? extends RecordBatch> batches;
    private final Set<Long> abortedProducerIds;
    private final PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions;
    private final FetchResponse.PartitionData<Records> partitionData;
    private final short responseVersion;

    private int recordsRead;
    private int bytesRead;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    private long nextFetchOffset;
    private Optional<Integer> lastEpoch;
    private boolean isConsumed = false;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private boolean initialized = false;
    private final IsolationLevel isolationLevel;
    private final boolean checkCrcs;
    private final Logger log;
    private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();
    private final Parser<ConsumerRecord<byte[], byte[]>> consumerRecordParser;

    public CompletedFetch(TopicPartition partition,
                          FetchResponse.PartitionData<Records> partitionData,
                          Iterator<? extends RecordBatch> batches,
                          Long fetchOffset,
                          short responseVersion,
                          IsolationLevel isolationLevel,
                          boolean checkCrcs,
                          Logger log) {
        this.partition = partition;
        this.partitionData = partitionData;
        this.batches = batches;
        this.nextFetchOffset = fetchOffset;
        this.responseVersion = responseVersion;
        this.lastEpoch = Optional.empty();
        this.abortedProducerIds = new HashSet<>();
        this.abortedTransactions = abortedTransactions(partitionData);
        this.isolationLevel = isolationLevel;
        this.checkCrcs = checkCrcs;
        this.log = log ;
        this.consumerRecordParser = createConsumerRecordParser(partition);
    }

    private void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            this.isConsumed = true;
        }
    }

    private void maybeEnsureValid(RecordBatch batch) {
        if (checkCrcs && currentBatch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record batch for partition " + partition + " at offset " +
                        batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeEnsureValid(Record record) {
        if (checkCrcs) {
            try {
                record.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            records.close();
            records = null;
        }
    }

    private Record nextFetchedRecord() {
        while (true) {
            if (records == null || !records.hasNext()) {
                maybeCloseRecordStream();

                if (!batches.hasNext()) {
                    // Message format v2 preserves the last offset in a batch even if the last record is removed
                    // through compaction. By using the next offset computed from the last offset in the batch,
                    // we ensure that the offset of the next fetch will point to the next batch, which avoids
                    // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    // fetching the same batch repeatedly).
                    if (currentBatch != null)
                        nextFetchOffset = currentBatch.nextOffset();
                    drain();
                    return null;
                }

                currentBatch = batches.next();
                lastEpoch = currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                        Optional.empty() : Optional.of(currentBatch.partitionLeaderEpoch());

                maybeEnsureValid(currentBatch);

                if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // remove from the aborted transaction queue all aborted transactions which have begun
                    // before the current batch's last offset and add the associated producerIds to the
                    // aborted producer set
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                    long producerId = currentBatch.producerId();
                    if (containsAbortMarker(currentBatch)) {
                        abortedProducerIds.remove(producerId);
                    } else if (isBatchAborted(currentBatch)) {
                        log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                        "offsets {} to {}",
                                partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                        nextFetchOffset = currentBatch.nextOffset();
                        continue;
                    }
                }

                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                Record record = records.next();
                // skip any records out of range
                if (record.offset() >= nextFetchOffset) {
                    // we only do validation when the message should not be skipped.
                    maybeEnsureValid(record);

                    // control records are not returned to the user
                    if (!currentBatch.isControlBatch()) {
                        return record;
                    } else {
                        // Increment the next fetch offset when we skip a control batch.
                        nextFetchOffset = record.offset() + 1;
                    }
                }
            }
        }
    }

    public static interface Parser<T> {
        T parse(RecordBatch recordBatch, Record record);
    }

    public final Parser<Record> unityParser = new Parser<Record>() {
        @Override
        public Record parse(RecordBatch recordBatch, Record record) {
            return record;
        }
    };

    private Parser<ConsumerRecord<byte[], byte[]>> createConsumerRecordParser(final TopicPartition lPartition) {
        return  new Parser<ConsumerRecord<byte[], byte[]>>() {
            @Override
            public ConsumerRecord<byte[], byte[]> parse(RecordBatch batch, Record record) {
                try {
                    long offset = record.offset();
                    long timestamp = record.timestamp();
                    Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
                    TimestampType timestampType = batch.timestampType();
                    Headers headers = new RecordHeaders(record.headers());
                    ByteBuffer keyBytes = record.key();
                    byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
                    ByteBuffer valueBytes = record.value();
                    byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
                    return new ConsumerRecord<>(lPartition.topic(), lPartition.partition(), offset,
                            timestamp, timestampType, record.checksumOrNull(),
                            keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                            valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                            keyByteArray, valueByteArray, headers, leaderEpoch);
                } catch (RuntimeException e) {
                    throw new SerializationException("Error deserializing key/value for partition " + lPartition +
                            " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
                }
            }
        };
    }


    public List<ConsumerRecord<byte[], byte[]>> fetchConsumerRecords(int maxRecords) {
        return fetchRecords(maxRecords, consumerRecordParser);
    }

    public ArrayList<Record> fetchRecords(int maxRecords) {
        return fetchRecords(maxRecords, unityParser);
    }

    private <T> ArrayList<T> fetchRecords(int maxRecords, Parser<T> parser) {
        // Error when fetching the next record before deserialization.
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);

        if (isConsumed)
            return new ArrayList<>();

        ArrayList<T> records = new ArrayList<>();
        try {
            for (int i = 0; i < maxRecords; i++) {
                // Only move to next record if there was no exception in the last fetch. Otherwise we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    lastRecord = nextFetchedRecord();
                    corruptLastRecord = false;
                }
                if (lastRecord == null)
                    break;
                records.add(parser.parse(currentBatch, lastRecord));
                recordsRead++;
                bytesRead += lastRecord.sizeInBytes();
                nextFetchOffset = lastRecord.offset() + 1;
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (records.isEmpty())
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (records.isEmpty())
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        }
        return records;
    }

    private void consumeAbortedTransactionsUpTo(long offset) {
        if (abortedTransactions == null)
            return;

        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset <= offset) {
            FetchResponse.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            abortedProducerIds.add(abortedTransaction.producerId);
        }
    }

    private boolean isBatchAborted(RecordBatch batch) {
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }

    private PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions(FetchResponse.PartitionData<?> partition) {
        if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
            return null;

        PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                partition.abortedTransactions().size(), Comparator.comparingLong(o -> o.firstOffset)
        );
        abortedTransactions.addAll(partition.abortedTransactions());
        return abortedTransactions;
    }

    private boolean containsAbortMarker(RecordBatch batch) {
        if (!batch.isControlBatch())
            return false;

        Iterator<Record> batchIterator = batch.iterator();
        if (!batchIterator.hasNext())
            return false;

        Record firstRecord = batchIterator.next();
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    }

    private boolean notInitialized() {
        return !this.initialized;
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }
}