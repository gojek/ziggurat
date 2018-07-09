package timestamp;


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Instant;

public class TimestampTransformer implements Transformer<byte[], byte[], KeyValue<byte[], byte[]>> {

    private ProcessorContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<byte[], byte[]> transform(byte[] recordKey, byte[] recordValue) {
        long recordTimestamp = context.timestamp();
        long currentTime = Instant.now().toEpochMilli();
        long delay = currentTime - recordTimestamp;

        System.out.printf("#######delay: %d \n", delay);
        return KeyValue.pair(recordKey, recordValue);
    }

    @Override
    public KeyValue<byte[], byte[]> punctuate(long timestamp) {
        // We don't need any periodic actions in this transformer.  Returning null achieves that.
        return null;
    }

    @Override
    public void close() {
        // Not needed.
    }

}