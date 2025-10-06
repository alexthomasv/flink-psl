package com.example.dedup;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A minimal Flink streaming job that performs line-level deduplication using the RocksDB state
 * backend.
 *
 * <p><b>Input</b>: all files in a configured directory (non-recursive). Each line is treated as one
 * record. The last whitespace-separated token is interpreted as the content hash (e.g., a hex
 * MD5/SHA1).
 *
 * <p><b>Logic</b>: the pipeline extracts the hash from each line, keys the stream by that hash, and
 * uses keyed {@code ValueState<Boolean>} (stored in RocksDB) to remember whether the hash has been
 * seen before. On first sighting the record is marked as "seen"; subsequent sightings are flagged
 * as duplicates.
 *
 * <p><b>Output</b>: prints a label for each record—either {@code NEW:<line>} or {@code DUP:<line>}.
 * You can redirect this to a file sink if you prefer.
 *
 * <p><b>State backend</b>: the job should be run with {@code EmbeddedRocksDBStateBackend} (or by
 * setting Flink's configuration) to persist the keyed state in RocksDB.
 *
 * <p><b>Notes</b>:
 *
 * <ul>
 *   <li>Throughput scales with parallelism for parsing and hashing, and with the key distribution
 *       across subtasks. Dedup correctness is guaranteed per hash key due to keyBy().
 *   <li>Exactly-once semantics can be enabled via checkpointing; duplicates are then measured w.r.t
 *       the keyed state snapshot, not per-run process memory.
 * </ul>
 *
 * <p><b>How to run</b>:
 *
 * <pre>
 *   ./bin/flink run -c com.example.dedup.DedupBenchmark /path/to/flink-dedup-bench.jar
 * </pre>
 */
public class DedupBenchmark {

    // ✅ CHANGE THIS to your directory (use file:// for absolute local paths)
    private static final String DIRECTORY =
            "file:///home/ubuntu/flink-1.16.3/traces"; // e.g. file:///home/ubuntu/blk/

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use RocksDB for keyed state
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // (optional) enable checkpoints if you want
        // env.enableCheckpointing(10_000L);
        // env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-chk");

        // Read ALL files in the given directory (non-recursive)
        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new Path(DIRECTORY) // directory, not file
                                )
                        .build();

        DataStreamSource<String> lines =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "dir-lines");

        SingleOutputStreamOperator<String> out =
                lines.flatMap(
                                (String line, Collector<Record> c) -> {
                                    if (line == null) {
                                        return;
                                    }
                                    String s = line.trim();
                                    if (s.isEmpty() || s.startsWith("#")) {
                                        return;
                                    }
                                    String[] toks = s.split("\\s+");
                                    if (toks.length < 2) {
                                        return;
                                    }
                                    String hash = toks[toks.length - 1];
                                    c.collect(new Record(hash, s));
                                })
                        .returns(Types.POJO(Record.class))
                        .keyBy(r -> r.hash)
                        .process(new DedupByHash());

        out.print().name("print");
        env.execute("RocksDB Dedup (hardcoded dir)");
    }

    /**
     * Immutable POJO representing one parsed input line and its deduplication key (hash).
     *
     * <p>This class is used as the element type after parsing input lines. Flink treats it as a
     * POJO (Plain Old Java Object) so it can be efficiently serialized and used in keyed state.
     */
    public static class Record {
        public String hash;
        public String line;

        public Record() {}

        public Record(String hash, String line) {
            this.hash = hash;
            this.line = line;
        }
    }

    /**
     * Keyed process function that performs deduplication by remembering whether a given hash has
     * been seen before.
     *
     * <p><b>Keying</b>: upstream, the stream is {@code keyBy(record.hash)} so each instance of this
     * operator only sees one logical hash key. This allows using small, per-key state to track
     * first sighting.
     *
     * <p><b>State</b>: a {@code ValueState<Boolean>} named {@code "seen"} indicates whether the key
     * has been observed. The state is stored in the configured state backend (RocksDB).
     *
     * <p><b>Output</b>:
     *
     * <ul>
     *   <li>If the key is first seen: emits {@code "NEW:" + record.fullLine} and sets {@code
     *       seen=true}.
     *   <li>Else (duplicate): emits {@code "DUP:" + record.fullLine}.
     * </ul>
     */
    public static class DedupByHash extends KeyedProcessFunction<String, Record, String> {
        private transient ValueState<Boolean> seen;

        @Override
        public void open(Configuration parameters) {
            seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Types.BOOLEAN));
        }

        @Override
        public void processElement(Record value, Context ctx, Collector<String> out)
                throws Exception {
            Boolean isSeen = seen.value();
            if (isSeen == null) {
                seen.update(true);
                out.collect("NEW  hash=" + value.hash + " | " + value.line);
            } else {
                out.collect("DUPL hash=" + value.hash);
            }
        }
    }
}
