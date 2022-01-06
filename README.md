# Kafka Reader

With the reader interface, clients can "manually position" themselves within any topic-partition and read all messages from a specified offset onward. 
The Reader API for Java enables you to create Reader objects by specifying a topic-partition, start offset and optional end offset.
Default end offset is the latest offset available at the time of invocation of read method. API also support manually specifying end offset.

```
import info.gtaneja.kafka.reader.Reader;
import java.util.Properties;

Properties prop = <intialize the property to create client>
Reder reader = Reader.create(prop);

public static void readAllMessages(String topic,
    int partition, long startOffset, Reader reader) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Iterator<Record> iterator = reader.read(startOffset. topicPartition);
    while(iterator.hasNext()) {
        Record r = iterator.next();
    }
}
```

Reader is thread safe. One reader interface should be reused to read multiple topic/partitions.

```
import info.gtaneja.kafka.reader.Reader;
import java.util.Properties;

Properties prop = <intialize the property to create client>
Reder reader = Reader.create(prop);
List<CompletableFuture<Iterator<Record>> result = new ArrayList<>();
List<Integer> partition = <list of partition>
for( int partition : partitions) {
    CompletableFuture<Iterator<Record>> futureIterator = CompletableFuture.supplyAsync(() ->
        reader.read(new TopicPartition(topic, partition), startOffset));
    result.add(futureIterator)
} 
```

Because reader interface allows you read data from any topic partition without subscribing or assigning to specific topic partition,
single instance of reader should be shared by application.

One of the major limitation of KafkaConsumer is that it can only work with fixed deserializer.
If your use case involves reading data from multiple topic with different serializer, then you end up creating multiple consumer

Creating Java stream and using Deserializer and reusing same reader instance.
```
Itertor<Record> records = read.read(...)
ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
Stream<Record> recordStream = StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(records, Spliterator.ORDERED), false);
long resCount = recordStream.map( r -> Reader.recordToConsumerRecord(deserializer,
        deserializer, topic, topicPartition.partition(), r)).count();
```

You can look at class `info.gtaneja.kafka.reader.ReaderTest` for more information
