/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.psl.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Java translation of the Rust ClientWorker.
 *
 * <p>Concurrency model: Two bounded channels implemented with BlockingQueue: backpressure (checker
 * -> generator) and generator (generator -> checker).
 *
 * <p>Two tasks: checkerTask() and generatorTask().
 *
 * <p>You can launch via ClientWorker.launch(...).
 */
public class ClientWorker<Gen extends ClientWorker.PerWorkerWorkloadGenerator> {

    // --------- External contracts / stubs you should replace with your real implementations -----

    /** How to send bytes to a node and await replies, with access to ClientConfig. */
    public static final class PinnedClient {
        private final AtomicReference<ClientConfig> config;

        public PinnedClient(ClientConfig cfg) {
            this.config = new AtomicReference<>(cfg);
        }

        public AtomicReference<ClientConfig> getConfigRef() {
            return config;
        }

        /** Send request bytes to given node (blocking or quick), return true if accepted. */
        public boolean send(String node, MessageRef ref) {
            // TODO: implement your RPC send
            return true;
        }

        /** Wait for next reply from this node; Optional.empty() = error/timeout. */
        public Optional<ReceivedMessage> awaitReply(String node) {
            // TODO: implement your RPC receive
            return Optional.empty();
        }
    }

    /** Typed wrapper for a byte payload. */
    public static final class MessageRef {
        public final byte[] buf;
        public final int len;
        public final SenderType senderType;

        public MessageRef(byte[] buf, int len, SenderType senderType) {
            this.buf = buf;
            this.len = len;
            this.senderType = senderType;
        }
    }

    /** Type of sender. */
    public enum SenderType {
        ANON
    }

    /** Output of RPC receive. */
    public static final class ReceivedMessage {
        public final byte[] bytes;
        public final int length;

        public ReceivedMessage(byte[] bytes, int length) {
            this.bytes = bytes;
            this.length = length;
        }
    }

    /** Your job config. */
    public static final class ClientConfig {
        public final WorkloadConfig workloadConfig;
        public final NetConfig netConfig;

        public ClientConfig(WorkloadConfig w, NetConfig n) {
            this.workloadConfig = w;
            this.netConfig = n;
        }

        public ClientConfig copyWith(NetConfig newNet) {
            return new ClientConfig(workloadConfig, newNet);
        }
    }

    /** For workload control (duration, max inflight). */
    public static final class WorkloadConfig {
        public final int durationSeconds;
        public final int maxConcurrentRequests;

        public WorkloadConfig(int durationSeconds, int maxConcurrentRequests) {
            this.durationSeconds = durationSeconds;
            this.maxConcurrentRequests = maxConcurrentRequests;
        }
    }

    /** Node topology. */
    public static final class NetConfig {
        public final String name; // my client name
        public final LinkedHashMap<String, Node> nodes; // nodeName -> Node

        public NetConfig(String name, LinkedHashMap<String, Node> nodes) {
            this.name = name;
            this.nodes = nodes;
        }

        public NetConfig copyWith(LinkedHashMap<String, Node> newNodes) {
            return new NetConfig(name, newNodes);
        }
    }

    /** Node. */
    public static final class Node {
        public final String name;

        public Node(String name) {
            this.name = name;
        }
    }

    /** What the generator yields per request. */
    public interface PerWorkerWorkloadGenerator {
        Generated next();
    }

    /** Generator output: the executor policy + tx bytes. */
    public static final class Generated {
        public final Executor executor;
        public final byte[] tx;

        public Generated(Executor executor, byte[] tx) {
            this.executor = executor;
            this.tx = tx;
        }
    }

    /** Executor type. */
    public enum Executor {
        Leader,
        Any
    }

    /** Stats channel payloads (replace with your own metric sinks). */
    public interface ClientWorkerStat {}

    /** Latency of a Byzantine commit. */
    public static final class ByzCommitLatency implements ClientWorkerStat {
        public final Duration latency;

        public ByzCommitLatency(Duration d) {
            this.latency = d;
        }
    }

    /** Latency of a crash commit. */
    public static final class CrashCommitLatency implements ClientWorkerStat {
        public final Duration latency;

        public CrashCommitLatency(Duration d) {
            this.latency = d;
        }
    }

    /** Pending count of a Byzantine commit. */
    public static final class ByzCommitPending implements ClientWorkerStat {
        public final int workerId;
        public final int pendingCount;

        public ByzCommitPending(int workerId, int pendingCount) {
            this.workerId = workerId;
            this.pendingCount = pendingCount;
        }
    }

    /** Simplified "sender" using a BlockingQueue. */
    public static final class Sender<T> {
        private final BlockingQueue<T> q;

        Sender(BlockingQueue<T> q) {
            this.q = q;
        }

        public void send(T v) throws InterruptedException {
            q.put(v);
        }
    }

    /** Simplified "receiver" using a BlockingQueue. */
    public static final class Receiver<T> {
        private final BlockingQueue<T> q;

        Receiver(BlockingQueue<T> q) {
            this.q = q;
        }

        public T recv() throws InterruptedException {
            return q.take();
        }
    }

    /** Pair of sender/receiver, bounded capacity. */
    public static final class Channel<T> {
        public final Sender<T> sender;
        public final Receiver<T> receiver;

        public Channel(int capacity) {
            BlockingQueue<T> q = new ArrayBlockingQueue<>(capacity);
            this.sender = new Sender<>(q);
            this.receiver = new Receiver<>(q);
        }
    }

    // --- Protobuf-like stubs: replace with your generated classes and decoding ---

    /** Encoded payload. */
    public static final class ProtoPayload {
        public final byte[] encoded; // entire encoded payload

        public ProtoPayload(byte[] encoded) {
            this.encoded = encoded;
        }
    }

    /** Request from a client. */
    public static final class ProtoClientRequest {
        public final byte[] tx; // your message bytes
        public final String origin;
        public final byte[] sig;
        public final long clientTag;

        public ProtoClientRequest(byte[] tx, String origin, byte[] sig, long clientTag) {
            this.tx = tx;
            this.origin = origin;
            this.sig = sig;
            this.clientTag = clientTag;
        }
    }

    /** Reply to a client request. */
    public static final class ProtoClientReply {
        public final Reply reply;

        public ProtoClientReply(Reply reply) {
            this.reply = reply;
        }

        /** Leader message. */
        public static class LeaderMsg {
            public final String name;
            public final byte[] serializedNodeInfos;

            public LeaderMsg(String name, byte[] serializedNodeInfos) {
                this.name = name;
                this.serializedNodeInfos = serializedNodeInfos;
            }
        }

        /** Byzantine response. */
        public static class ByzResponse {
            public final long clientTag;

            public ByzResponse(long clientTag) {
                this.clientTag = clientTag;
            }
        }

        /** Receipt. */
        public static class Receipt {
            public final List<ByzResponse> byzResponses;

            public Receipt(List<ByzResponse> lst) {
                this.byzResponses = lst;
            }
        }

        /** Reply. */
        public interface Reply {}

        /** Receipt reply. */
        public static final class RReceipt implements Reply {
            public final Receipt r;

            public RReceipt(Receipt r) {
                this.r = r;
            }
        }

        /** Try again reply. */
        public static final class RTryAgain implements Reply {}

        /** Tentative receipt reply. */
        public static final class RTentativeReceipt implements Reply {}

        /** Leader reply. */
        public static final class RLeader implements Reply {
            public final LeaderMsg leader;

            public RLeader(LeaderMsg m) {
                this.leader = m;
            }
        }

        /** Decode from bytes -> reply. Replace with actual protobuf decoding. */
        public static Optional<ProtoClientReply> decode(byte[] buf, int len) {
            // TODO: real decode
            return Optional.empty();
        }
    }

    /** Deserialize node info list; replace with your real format. */
    public static final class NodeInfo {
        /** Nodes. */
        public final LinkedHashMap<String, Node> nodes;

        public NodeInfo(LinkedHashMap<String, Node> nodes) {
            this.nodes = nodes;
        }

        /** Deserialize node info list. */
        public static NodeInfo deserialize(byte[] raw) {
            // TODO: real deserialization
            return new NodeInfo(new LinkedHashMap<>());
        }
    }

    // --------------------------------- Actual translation ----------------------------------------

    private final ClientConfig config;
    private final PinnedClient client;
    private final Gen generator;
    private final int id;
    private final Sender<ClientWorkerStat> statTx;

    public ClientWorker(
            ClientConfig config,
            PinnedClient client,
            Gen generator,
            int id,
            Sender<ClientWorkerStat> statTx) {
        this.config = config;
        this.client = client;
        this.generator = generator;
        this.id = id;
        this.statTx = statTx;
    }

    /** Handle to the running worker tasks. */
    public static final class WorkerHandle {
        /** Checker future. */
        public final Future<?> checkerFuture;

        public final Future<?> generatorFuture;
        /** Generator future. */
        public WorkerHandle(Future<?> c, Future<?> g) {
            this.checkerFuture = c;
            this.generatorFuture = g;
        }
    }

    /** Launch both tasks on the provided executor. */
    public WorkerHandle launch(ExecutorService exec) {
        final int maxOutstanding = config.workloadConfig.maxConcurrentRequests;

        // backpressure channel: checker -> generator
        Channel<CheckerResponse> backpressure = new Channel<>(maxOutstanding);
        // generator channel: generator -> checker
        Channel<CheckerTask> generatorCh = new Channel<>(maxOutstanding);

        // Pre-fill backpressure with "Success tokens" so generator can issue first N requests.
        try {
            for (int i = 0; i < maxOutstanding; i++) {
                backpressure.sender.send(CheckerResponse.success(0));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while pre-filling backpressure", ie);
        }

        Future<?> checkerF =
                exec.submit(
                        () ->
                                checkerTask(
                                        backpressure.sender,
                                        generatorCh.receiver,
                                        client,
                                        statTx,
                                        id));
        Future<?> generatorF =
                exec.submit(
                        () ->
                                generatorTask(
                                        generatorCh.sender,
                                        backpressure.receiver,
                                        backpressure.sender,
                                        id));

        return new WorkerHandle(checkerF, generatorF);
    }

    // ----- Data types used between tasks -----

    private static final class CheckerTask {
        final Instant startTime;
        final String waitFrom;
        final long id;
        final Executor executorMode;

        CheckerTask(Instant startTime, String waitFrom, long id, Executor mode) {
            this.startTime = startTime;
            this.waitFrom = waitFrom;
            this.id = id;
            this.executorMode = mode;
        }
    }

    private static final class OutstandingRequest {
        long id;
        byte[] payload; // encoded ProtoPayload
        Executor executorMode;
        String lastSentTo;
        Instant startTime;

        CheckerTask toCheckerTask() {
            return new CheckerTask(startTime, lastSentTo, id, executorMode);
        }
    }

    /** Either Success(requestId) or TryAgain(task, maybeNewNodeList, maybeNewLeaderIdx). */
    private static final class CheckerResponse {
        final boolean success;
        final long successId;

        final CheckerTask tryTask;
        final List<String> newNodeList; // may be null
        final Integer newLeaderIndex; // may be null

        private CheckerResponse(
                boolean success,
                long successId,
                CheckerTask tryTask,
                List<String> newNodeList,
                Integer newLeaderIndex) {
            this.success = success;
            this.successId = successId;
            this.tryTask = tryTask;
            this.newNodeList = newNodeList;
            this.newLeaderIndex = newLeaderIndex;
        }

        static CheckerResponse success(long id) {
            return new CheckerResponse(true, id, null, null, null);
        }

        static CheckerResponse tryAgain(CheckerTask t, List<String> nodes, Integer leaderIdx) {
            return new CheckerResponse(false, 0L, t, nodes, leaderIdx);
        }
    }

    // ----- Checker task -----

    private void checkerTask(
            Sender<CheckerResponse> backpressureTx,
            Receiver<CheckerTask> generatorRx,
            PinnedClient client,
            Sender<ClientWorkerStat> statTx,
            int workerId) {
        final Map<Long, CheckerTask> waitingForByz = new HashMap<>();
        final Map<Long, Instant> outOfOrderByz = new HashMap<>();
        String allegedLeader = "";

        while (!Thread.currentThread().isInterrupted()) {
            CheckerTask req;
            try {
                req = generatorRx.recv();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }

            if (req == null) {
                break;
            }
            if (allegedLeader.isEmpty() && req.executorMode == Executor.Leader) {
                allegedLeader = req.waitFrom;
            }
            if (!allegedLeader.equals(req.waitFrom) && req.executorMode == Executor.Leader) {
                // "Leader changed while we are waiting for this request" – keep note
                // (In Rust they trace-log this; here we just comment)
            }

            // Was there an out-of-order byz response already recorded?
            Instant t = outOfOrderByz.remove(req.id);
            if (t != null) {
                if (t.isAfter(req.startTime)) {
                    Duration lat = Duration.between(req.startTime, t);
                    try {
                        statTx.send(new ByzCommitLatency(lat));
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    // log error: received before send-time
                }
            } else {
                if (req.executorMode == Executor.Leader) {
                    waitingForByz.put(req.id, req);
                }
            }

            if (req.executorMode == Executor.Leader) {
                try {
                    statTx.send(new ByzCommitPending(workerId, waitingForByz.size()));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }

            // Await reply for this request from req.waitFrom
            Optional<ReceivedMessage> maybeMsg = client.awaitReply(req.waitFrom);
            if (!maybeMsg.isPresent()) {
                // ask generator to retry this same request
                try {
                    backpressureTx.send(CheckerResponse.tryAgain(req, null, null));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            ReceivedMessage msg = maybeMsg.get();

            Optional<ProtoClientReply> decoded = ProtoClientReply.decode(msg.bytes, msg.length);
            if (!decoded.isPresent()) {
                try {
                    backpressureTx.send(CheckerResponse.tryAgain(req, null, null));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            ProtoClientReply reply = decoded.get();
            if (reply.reply instanceof ProtoClientReply.RReceipt) {
                // success
                try {
                    backpressureTx.send(CheckerResponse.success(req.id));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                try {
                    statTx.send(
                            new CrashCommitLatency(Duration.between(req.startTime, Instant.now())));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }

                ProtoClientReply.Receipt r = ((ProtoClientReply.RReceipt) reply.reply).r;
                for (ProtoClientReply.ByzResponse br : r.byzResponses) {
                    CheckerTask tsk = waitingForByz.remove(br.clientTag);
                    if (tsk != null) {
                        Duration lat = Duration.between(tsk.startTime, Instant.now());
                        try {
                            statTx.send(new ByzCommitLatency(lat));
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        outOfOrderByz.put(br.clientTag, Instant.now());
                    }
                }
            } else if (reply.reply instanceof ProtoClientReply.RTryAgain) {
                try {
                    backpressureTx.send(CheckerResponse.tryAgain(req, null, null));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            } else if (reply.reply instanceof ProtoClientReply.RTentativeReceipt) {
                // treat as success
                try {
                    backpressureTx.send(CheckerResponse.success(req.id));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                try {
                    statTx.send(
                            new CrashCommitLatency(Duration.between(req.startTime, Instant.now())));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            } else if (reply.reply instanceof ProtoClientReply.RLeader) {
                ProtoClientReply.LeaderMsg lm = ((ProtoClientReply.RLeader) reply.reply).leader;
                String currLeader = lm.name;

                NodeInfo ni = NodeInfo.deserialize(lm.serializedNodeInfos);
                List<String> nodeList = new ArrayList<>(ni.nodes.keySet());
                Collections.sort(nodeList);

                int newLeaderIdx = nodeList.indexOf(currLeader);
                if (newLeaderIdx < 0) {
                    // malformed: leader not in node list
                    try {
                        backpressureTx.send(CheckerResponse.tryAgain(req, null, null));
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                // Update client config (like Arc<...> write in Rust)
                ClientConfig oldCfg = client.getConfigRef().get();

                // first update the NetConfig with the new nodes:
                NetConfig updatedNet = oldCfg.netConfig.copyWith(ni.nodes);

                // then build a new ClientConfig using the updated NetConfig:
                ClientConfig newCfg = oldCfg.copyWith(updatedNet);

                client.getConfigRef().set(newCfg);
                client.getConfigRef().set(newCfg);

                if (!Objects.equals(allegedLeader, currLeader)) {
                    waitingForByz.clear();
                    outOfOrderByz.clear();
                    allegedLeader = currLeader;
                }

                try {
                    backpressureTx.send(CheckerResponse.tryAgain(req, nodeList, newLeaderIdx));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }

            } else {
                // None => try again
                try {
                    backpressureTx.send(CheckerResponse.tryAgain(req, null, null));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // ----- Generator task -----

    private void generatorTask(
            Sender<CheckerTask> generatorTx,
            Receiver<CheckerResponse> backpressureRx,
            Sender<CheckerResponse> backpressureTx,
            int workerId) {
        final Map<Long, OutstandingRequest> outstanding = new HashMap<>();
        long totalRequests = 0;

        // snapshot and sort node names
        List<String> nodeList = new ArrayList<>(config.netConfig.nodes.keySet());
        Collections.sort(nodeList);

        int currLeaderIdx = 0;
        int currRRIdx = workerId % Math.max(nodeList.size(), 1);

        final String myName = config.netConfig.name;

        final Duration sleep1s = Duration.ofSeconds(1);
        sleep(sleep1s);

        Duration backoffTime = Duration.ofMillis(1000);
        int currComplaining = 0;
        int maxInflight = config.workloadConfig.maxConcurrentRequests;

        Instant globalStart = Instant.now();
        Duration runFor = Duration.ofSeconds(config.workloadConfig.durationSeconds);

        while (Duration.between(globalStart, Instant.now()).compareTo(runFor) < 0
                && !Thread.currentThread().isInterrupted()) {
            CheckerResponse signal;
            try {
                signal = backpressureRx.recv();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            if (signal == null) {
                break;
            }
            if (signal.success) {
                // Consume success token, maybe remove old req id
                if (signal.successId != 0) {
                    outstanding.remove(signal.successId);
                }

                // Create and send a new request
                Generated g = generator.next();
                OutstandingRequest req = new OutstandingRequest();
                req.id = totalRequests + 1;
                req.executorMode = g.executor;
                req.startTime = Instant.now();
                req.payload =
                        encodeClientRequest(
                                new ProtoClientRequest(g.tx, myName, new byte[] {0x00}, req.id));
                // Send it
                sendRequest(
                        req,
                        nodeList,
                        currLeaderIdxHolder(currLeaderIdx),
                        rrIdxHolder(currRRIdx),
                        outstanding);
                // Track + tell checker
                try {
                    generatorTx.send(req.toCheckerTask());
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                outstanding.put(req.id, req);
                totalRequests += 1;

            } else {
                // TryAgain
                CheckerTask task = signal.tryTask;
                boolean leaderChangedByServer = (signal.newLeaderIndex != null);

                if (!leaderChangedByServer) {
                    // local backoff on generic TryAgain
                    if (currComplaining == 0) {
                        sleep(backoffTime);
                    }
                    currComplaining++;
                    if (currComplaining >= maxInflight) {
                        currComplaining = 0;
                    }
                }

                String oldLeaderName = nodeList.isEmpty() ? "" : nodeList.get(currLeaderIdx);

                if (signal.newLeaderIndex != null) {
                    currLeaderIdx = signal.newLeaderIndex;
                }
                if (signal.newNodeList != null) {
                    nodeList = new ArrayList<>(signal.newNodeList);
                }

                String newLeaderName = nodeList.isEmpty() ? "" : nodeList.get(currLeaderIdx);
                if (!Objects.equals(oldLeaderName, newLeaderName)) {
                    outstanding.clear();
                }

                OutstandingRequest req = outstanding.remove(task.id);
                if (req == null) {
                    // checker told us to retry a request we no longer track -> release one token
                    try {
                        backpressureTx.send(CheckerResponse.success(0));
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                sendRequest(
                        req,
                        nodeList,
                        currLeaderIdxHolder(currLeaderIdx),
                        rrIdxHolder(currRRIdx),
                        outstanding);
                try {
                    generatorTx.send(req.toCheckerTask());
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                outstanding.put(req.id, req);
            }
        }

        // done
        // (Rust logs final totals; you can add logging)
    }

    // Small holders to pass primitive ints by reference
    private static int[] currLeaderIdxHolder(int v) {
        return new int[] {v};
    }

    private static int[] rrIdxHolder(int v) {
        return new int[] {v};
    }

    private void sendRequest(
            OutstandingRequest req,
            List<String> nodeList,
            int[] currLeaderIdx,
            int[] currRRIdx,
            Map<Long, OutstandingRequest> outstanding) {
        final byte[] buf = req.payload;
        final int len = buf.length;

        Executor mode = req.executorMode;
        while (true) {
            boolean ok;
            if (mode == Executor.Leader) {
                if (nodeList.isEmpty()) {
                    ok = false;
                } else {
                    String node = nodeList.get(Math.floorMod(currLeaderIdx[0], nodeList.size()));
                    req.lastSentTo = node;
                    ok = client.send(node, new MessageRef(buf, len, SenderType.ANON));
                }
            } else { // Any
                if (nodeList.isEmpty()) {
                    ok = false;
                } else {
                    String node = nodeList.get(Math.floorMod(currRRIdx[0], nodeList.size()));
                    req.lastSentTo = node;
                    ok = client.send(node, new MessageRef(buf, len, SenderType.ANON));
                }
            }

            if (!ok) {
                // rotate and clear outstanding (like the Rust version)
                if (mode == Executor.Leader) {
                    currLeaderIdx[0] = (currLeaderIdx[0] + 1) % Math.max(nodeList.size(), 1);
                } else {
                    currRRIdx[0] = (currRRIdx[0] + 1) % Math.max(nodeList.size(), 1);
                }
                outstanding.clear();
                continue;
            }
            break;
        }
    }

    // --------------------------------- helpers ---------------------------------

    private static void sleep(Duration d) {
        try {
            Thread.sleep(Math.max(1L, d.toMillis()));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** Replace with your protobuf encoding of client request into a payload. */
    private static byte[] encodeClientRequest(ProtoClientRequest req) {
        // TODO: real encode
        return new byte[] {};
    }
}
