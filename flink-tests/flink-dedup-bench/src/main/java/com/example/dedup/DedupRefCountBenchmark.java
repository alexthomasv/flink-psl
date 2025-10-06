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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink streaming job that performs content-based deduplication over line-oriented files, using
 * RocksDB for keyed state. It supports:
 *
 * <ul>
 *   <li><b>Global dedup</b> (key by hash): maintain a refcount per fingerprint.
 *   <li><b>Overwrite-aware dedup</b> (key by LBA): track last hash per LBA and adjust global
 *       refcounts on overwrite.
 * </ul>
 *
 * <p>Input: all files in a configured directory (non-recursive). Each non-empty line is expected to
 * end with a content fingerprint (last whitespace-separated token).
 *
 * <p>State backend: Embedded RocksDB. Enable checkpointing for exactly-once state updates.
 */
public class DedupRefCountBenchmark {

    // CHANGE THIS to your directory with blkparse-like files
    private static final String DIRECTORY = "file:///home/ubuntu/flink-1.16.3/traces";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Use RocksDB for keyed state
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        // Optionally:
        // env.enableCheckpointing(10_000);

        // 1) Source: read all files (non-recursive) as lines
        FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(DIRECTORY))
                        .build();

        DataStreamSource<String> lines =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "dir-lines");

        // 2) Parse → keep only WRITES → (LBA, hash)
        DataStream<WriteOp> writes =
                lines.flatMap(
                                (String line, Collector<WriteOp> out) -> {
                                    if (line == null) {
                                        return;
                                    }
                                    String s = line.trim();
                                    if (s.isEmpty() || s.startsWith("#")) {
                                        return;
                                    }
                                    // expected: ts pid proc LBA size OP ... HASH
                                    String[] t = s.split("\\s+");
                                    if (t.length < 8) {
                                        return;
                                    }

                                    String op = t[5];
                                    if (!"W".equalsIgnoreCase(op)) {
                                        return;
                                    } // only writes

                                    // LBA is typically at index 3
                                    long lba;
                                    try {
                                        lba = Long.parseLong(t[3]);
                                    } catch (NumberFormatException e) {
                                        return;
                                    }

                                    String hash = t[t.length - 1]; // last token
                                    if (hash.isEmpty()) {
                                        return;
                                    }

                                    out.collect(new WriteOp(lba, hash, s));
                                })
                        .returns(Types.POJO(WriteOp.class));

        // 3) key by LBA → track current resident hash per LBA, emit +/- deltas
        DataStream<Delta> deltas =
                writes.keyBy(w -> w.lba).process(new LBAStateFn()).name("LBA->Delta");

        // 4) key by fingerprint → maintain refcount
        DataStream<String> events =
                deltas.keyBy(d -> d.hash).process(new RefCountFn()).name("RefCount");

        // 5) print transitions
        events.print().name("print");

        env.execute("RocksDB Dedup RefCount (writes-only)");
    }

    /** Parsed write op. */
    public static class WriteOp {
        public long lba;
        public String hash;
        public String raw; // original line (optional, handy for debugging)

        public WriteOp() {}

        public WriteOp(long lba, String hash, String raw) {
            this.lba = lba;
            this.hash = hash;
            this.raw = raw;
        }
    }

    /** Delta event: increment (+1) or decrement (−1) a fingerprint refcount. */
    public static class Delta {
        public String hash;
        public int delta; // +1 or -1

        public Delta() {}

        public Delta(String hash, int delta) {
            this.hash = hash;
            this.delta = delta;
        }
    }

    /**
     * Overwrite-aware inline dedup simulator keyed by LBA. Tracks last hash per LBA and adjusts
     * global hash refcounts on overwrite.
     *
     * <p>KeyBy: Record.lba
     *
     * <p>State: ValueState&lt;String&gt; "currentHash" (last fingerprint for this LBA).
     *
     * <p>On write:
     *
     * <ul>
     *   <li>No prior hash → increment globalRef[new]; set currentHash=new
     *   <li>Same hash → no-op
     *   <li>Different hash → decrement globalRef[old], increment globalRef[new]; update currentHash
     * </ul>
     */
    public static class LBAStateFn extends KeyedProcessFunction<Long, WriteOp, Delta> {
        private transient ValueState<String> currentHash;

        @Override
        public void open(Configuration parameters) {
            currentHash =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("current-hash", Types.STRING));
        }

        @Override
        public void processElement(WriteOp w, Context ctx, Collector<Delta> out) throws Exception {
            String old = currentHash.value();
            String now = w.hash;

            if (old == null) {
                // first time this LBA gets a value → +1 for new fingerprint
                currentHash.update(now);
                out.collect(new Delta(now, +1));
            } else if (!old.equals(now)) {
                // overwrite: decrement old, increment new
                out.collect(new Delta(old, -1));
                out.collect(new Delta(now, +1));
                currentHash.update(now);
            }
            // else same fingerprint; do nothing
        }
    }

    /**
     * RefcountFn (two-input, overwrite-aware).
     *
     * <p>Purpose: Maintain per-hash refcounts where <em>only the current</em> hash at each LBA
     * contributes. {@code LBAStateFn} decides whether an event is {@code INC(hash)} or {@code
     * DEC(hash)} and sends commands here. This operator applies the commands atomically per hash.
     *
     * <p><b>KeyBy</b>: {@code command.hash} (so all updates for a given hash are serialized)
     *
     * <p><b>Inputs</b>:
     *
     * <ul>
     *   <li>stream A: {@code IncRef(hash)} commands
     *   <li>stream B: {@code DecRef(hash)} commands
     * </ul>
     *
     * <p><b>State (RocksDB)</b>: {@code ValueState<Long>} refCount
     *
     * <p><b>Behavior</b>:
     *
     * <ul>
     *   <li>On {@code IncRef(H)}: if {@code refCount == null} → set to 1 and emit {@code NEW}; else
     *       increment and emit {@code REF++}.
     *   <li>On {@code DecRef(H)}: if {@code refCount == null} → ignore; else decrement and emit
     *       {@code REF--}; if it reaches 0, emit {@code GC hash=H}.
     * </ul>
     *
     * <p><b>Notes</b>:
     *
     * <ul>
     *   <li>Pair with {@code LBAStateFn} keyed by LBA that emits Inc/Dec on overwrites.
     *   <li>Consider state TTL or explicit GC once {@code refCount} hits zero.
     * </ul>
     */
    public static class RefCountFn extends KeyedProcessFunction<String, Delta, String> {
        private transient ValueState<Integer> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("refcount", Types.INT));
        }

        @Override
        public void processElement(Delta d, Context ctx, Collector<String> out) throws Exception {
            Integer c = count.value();
            if (c == null) {
                c = 0;
            }
            int newCount = c + d.delta;

            if (newCount < 0) {
                // Should not happen if deltas are balanced, but guard anyway.
                newCount = 0;
            }

            count.update(newCount);

            if (d.delta > 0) {
                if (newCount == 1) {
                    out.collect("REF NEW  hash=" + ctx.getCurrentKey() + " → count=1");
                } else {
                    out.collect("REF INC  hash=" + ctx.getCurrentKey() + " → count=" + newCount);
                }
            } else { // delta < 0
                if (newCount == 0) {
                    out.collect(
                            "REF ZERO hash=" + ctx.getCurrentKey() + " → count=0 (unreferenced)");
                } else {
                    out.collect("REF DEC  hash=" + ctx.getCurrentKey() + " → count=" + newCount);
                }
            }
        }
    }
}
