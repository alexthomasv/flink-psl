import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Demo job that exercises Flink's RocksDB state backend with large per-key state.
 *
 * <p><b>What it does</b>
 *
 * <ul>
 *   <li>Enables the {@link EmbeddedRocksDBStateBackend} and periodic checkpoints.
 *   <li>Creates a tiny in-memory source of a few strings and keys the stream by the string value.
 *   <li>Keeps two pieces of keyed state:
 *       <ul>
 *         <li>a small {@link ValueState}&lt;Long&gt; counter per key, and
 *         <li>a large {@code byte[]} blob (~1 MiB) per key to stress RocksDB I/O.
 *       </ul>
 *   <li>On each element, increments the counter and rewrites the 1 MiB blob, then emits the count.
 * </ul>
 *
 * <p><b>Why</b> <br>
 * Useful as a smoke/stress test to verify that RocksDB initializes, writes/reads state, and
 * interacts with checkpointing under heavier state payloads.
 *
 * <p><b>Notes</b>
 *
 * <ul>
 *   <li>Large state increases checkpoint time and disk usage; tune checkpointing and RocksDB
 *       options as needed.
 *   <li>Reduce {@code BLOB_SIZE} if the test is too heavy for your environment.
 * </ul>
 */
public class CountPerKeyRocks {
    private static final int BLOB_SIZE = 1 << 20; // 1 MiB

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.enableCheckpointing(5000);

        DataStream<String> src =
                env.fromElements("a", "b", "a", "a", "c", "b", "a", "c", "c", "c", "b", "a");

        src.keyBy(s -> s)
                .process(
                        new KeyedProcessFunction<String, String, String>() {
                            private transient ValueState<byte[]> blobState; // 1 MiB per key
                            private transient ValueState<Long>
                                    countState; // keep a small counter too

                            @Override
                            public void open(
                                    org.apache.flink.configuration.Configuration parameters) {
                                blobState =
                                        getRuntimeContext()
                                                .getState(
                                                        new ValueStateDescriptor<>(
                                                                "big-blob", byte[].class));

                                countState =
                                        getRuntimeContext()
                                                .getState(
                                                        new ValueStateDescriptor<>(
                                                                "cnt", Long.class));
                            }

                            @Override
                            public void processElement(
                                    String value, Context ctx, Collector<String> out)
                                    throws Exception {
                                Long c = countState.value();
                                if (c == null) {
                                    c = 0L;
                                }
                                c++;
                                countState.update(c);

                                // Create/update a deterministic 1 MiB payload for this key+count
                                byte[] blob = blobState.value();
                                if (blob == null || blob.length != BLOB_SIZE) {
                                    blob = new byte[BLOB_SIZE];
                                }
                                // Fill with a simple pattern so it’s not all zeros
                                // First bytes: key + count (ASCII), rest: repeat a byte pattern
                                byte[] header = (value + "#" + c).getBytes(StandardCharsets.UTF_8);
                                Arrays.fill(blob, (byte) (value.hashCode() ^ c.intValue()));
                                System.arraycopy(
                                        header,
                                        0,
                                        blob,
                                        0,
                                        Math.min(header.length, Math.min(128, blob.length)));

                                blobState.update(blob);

                                out.collect(
                                        "key="
                                                + value
                                                + " count="
                                                + c
                                                + " (stored "
                                                + (blob.length / 1024)
                                                + " KiB in RocksDB)");
                            }
                        })
                .print();

        env.execute("rocksdb-smoke-1MiB");
    }
}
