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

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proto.client.Client;
import proto.execution.Execution;
import proto.rpc.Rpc;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Blocking KV client that uses {@link PinnedClient} as the transport.
 *
 * <p>Builds {@code ProtoTransaction} and wraps it into {@code ProtoClientRequest}/{@code
 * ProtoPayload}, sends via {@link PinnedClient#sendAndAwaitReply(String, PinnedClient.MessageRef)}
 * and parses {@code ProtoClientReply}. This version targets a specific node (either a default node
 * provided at construction or passed per call) and does not implement redirect/backoff logic.
 */
public final class KVSClient {

    private static final Logger LOG = LoggerFactory.getLogger(KVSClient.class);

    private final PinnedClient transport;
    private final String defaultNode;
    private final AtomicLong tagSeq = new AtomicLong(1L);
    private double pslLookupRate = 0.10;
    private boolean pslEnabled = false;
    private final int maxOutstanding = 50000;

    // ==== async reply-dispatch state ====
    private final ConcurrentHashMap<Long, CompletableFuture<byte[]>> pending =
            new ConcurrentHashMap<>();
    // one reply loop per node (spawned lazily)
    private final ConcurrentHashMap<String, Thread> replyLoops = new ConcurrentHashMap<>();

    // bounded in-flight requests across all nodes
    private final Semaphore maxInFlight;
    // lifecycle
    private volatile boolean running = true;
    private final ConcurrentHashMap<String, Semaphore> replyReady = new ConcurrentHashMap<>();

    /**
     * Get the reply ready semaphore for a given node.
     *
     * @param node the node name
     * @return the reply ready semaphore
     */
    public Semaphore readyFor(String node) {
        return replyReady.computeIfAbsent(node, n -> new Semaphore(0, true));
    }

    /**
     * Creates a KV client with a fixed default node to contact.
     *
     * @param transport initialized {@link PinnedClient}
     * @param defaultNode logical node name present in the transport's configuration (nullable if
     *     you plan to pass the node each call)
     */
    public KVSClient(final PinnedClient transport, final String defaultNode) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.transport.replyReady = this.replyReady;
        this.defaultNode = defaultNode;
        this.maxInFlight = new Semaphore(maxOutstanding, true);
        startCheckerThread(defaultNode);
    }

    /**
     * Get the PSL lookup rate.
     *
     * @return the PSL lookup rate
     */
    public double getLookupRate() {
        return pslLookupRate;
    }

    /**
     * Get the PSL lookup rate.
     *
     * @return the PSL lookup rate
     */
    public boolean isEnabled() {
        return pslEnabled;
    }

    /**
     * Set the PSL lookup rate.
     *
     * @param pslLookupRate the PSL lookup rate
     */
    public void setLookupRate(double pslLookupRate) {
        this.pslLookupRate = pslLookupRate;
    }

    /**
     * Set the PSL enable.
     *
     * @param pslEnabled the PSL enable
     */
    public void setEnable(boolean pslEnabled) {
        this.pslEnabled = pslEnabled;
    }

    /**
     * Creates a KV client without a default node; you must pass the node per call.
     *
     * @param transport initialized {@link PinnedClient}
     */
    public KVSClient(final PinnedClient transport) {
        this(transport, null);
    }

    // -------------------------------------------------------------------------------------------------
    // Public API (default-node variants)
    // -------------------------------------------------------------------------------------------------

    /**
     * PUT using the configured default node.
     *
     * @param key key bytes
     * @param value value bytes
     * @throws IOException on transport or parse error
     * @throws IllegalStateException if no default node was provided
     */
    public CompletableFuture<byte[]> put(final byte[] key, final byte[] value)
            throws IOException, InterruptedException {
        // LOG.info(
        //         "KVSClient.put key=0x{}, value=0x{}, size={}",
        //         key == null
        //                 ? "null"
        //                 : DatatypeConverter.printHexBinary(key).toLowerCase(Locale.ROOT),
        //         value == null
        //                 ? "null"
        //                 : DatatypeConverter.printHexBinary(value).toLowerCase(Locale.ROOT),
        //         value == null ? 0 : value.length);
        return put(defaultNode, key, value);
    }

    /**
     * GET using the configured default node.
     *
     * @param key key bytes
     * @return value bytes or {@code null} if missing
     * @throws IOException on transport or parse error
     * @throws IllegalStateException if no default node was provided
     */
    public CompletableFuture<byte[]> get(final byte[] key)
            throws IOException, InterruptedException {
        return get(defaultNode, key);
    }

    // -------------------------------------------------------------------------------------------------
    // Public API (explicit node)
    // -------------------------------------------------------------------------------------------------

    /**
     * Linearizable write (crash-commit) to a specific node.
     *
     * @param node logical node name
     * @param key key bytes
     * @param value value bytes
     * @throws IOException on transport or parse error
     */
    public CompletableFuture<byte[]> put(final String node, final byte[] key, final byte[] value)
            throws IOException, InterruptedException {
        final Execution.ProtoTransaction tx = buildWriteCrashCommitTx(key, value);
        final Client.ProtoClientRequest req = buildRequest(tx);
        final long tag = req.getClientTag();
        final byte[] payload =
                Rpc.ProtoPayload.newBuilder().setClientRequest(req).build().toByteArray();

        maxInFlight.acquire();
        CompletableFuture<byte[]> fut = new CompletableFuture<byte[]>();
        pending.put(tag, fut);
        transport.send(node, new PinnedClient.MessageRef(payload));
        return fut;
    }

    /**
     * Read from a specific node.
     *
     * @param node logical node name
     * @param key key bytes
     * @return value bytes or {@code null} if not found / no value in receipt
     * @throws IOException on transport or parse error
     */
    public CompletableFuture<byte[]> get(final String node, final byte[] key)
            throws IOException, InterruptedException {
        final Execution.ProtoTransaction tx = buildReadOnReceiveTx(key);
        final Client.ProtoClientRequest req = buildRequest(tx);
        final byte[] payload =
                Rpc.ProtoPayload.newBuilder().setClientRequest(req).build().toByteArray();
        final long tag = req.getClientTag();

        // final PinnedClient.PinnedMessage msg =
        //         transport.sendAndAwaitReply(node, new PinnedClient.MessageRef(payload));s
        maxInFlight.acquire();
        CompletableFuture<byte[]> fut = new CompletableFuture<byte[]>();
        pending.put(tag, fut);
        transport.send(node, new PinnedClient.MessageRef(payload));
        return fut;
        // try {
        //     final Client.ProtoClientReply reply =
        //             Client.ProtoClientReply.parseFrom(
        //                     CodedInputStream.newInstance(msg.buf, 0, msg.length));

        //     if (reply.hasReceipt() && reply.getReceipt().hasResults()) {
        //         final Execution.ProtoTransactionResult tr = reply.getReceipt().getResults();
        //         if (tr.getResultCount() > 0) {
        //             final Execution.ProtoTransactionOpResult opRes = tr.getResult(0);
        //             if (opRes.getValuesCount() > 0) {
        //                 return opRes.getValues(0).toByteArray();
        //             }
        //         }
        //     }
        //     return null;
        // } catch (final Exception parse) {
        //     throw new IOException("Failed to parse ProtoClientReply", parse);
        // }
    }

    // -------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------

    private Client.ProtoClientRequest buildRequest(final Execution.ProtoTransaction tx) {
        final long tag = tagSeq.getAndIncrement();
        return Client.ProtoClientRequest.newBuilder()
                .setTx(tx)
                .setOrigin("client") // TODO: wire your actual client identity if needed
                .setSig(ByteString.copyFrom(new byte[] {0})) // TODO: real signature if required
                .setClientTag(tag)
                .build();
    }

    private static Execution.ProtoTransaction buildReadOnReceiveTx(final byte[] key) {
        final Execution.ProtoTransactionOp read =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.READ)
                        .addOperands(ByteString.copyFrom(key))
                        .build();
        final Execution.ProtoTransactionPhase onReceive =
                Execution.ProtoTransactionPhase.newBuilder().addOps(read).build();
        return Execution.ProtoTransaction.newBuilder().setOnReceive(onReceive).build();
    }

    private static Execution.ProtoTransaction buildWriteCrashCommitTx(
            final byte[] key, final byte[] value) {
        final Execution.ProtoTransactionOp write =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.WRITE)
                        .addOperands(ByteString.copyFrom(key))
                        .addOperands(ByteString.copyFrom(value))
                        .build();
        final Execution.ProtoTransactionPhase onReceive =
                Execution.ProtoTransactionPhase.newBuilder().addOps(write).build();
        return Execution.ProtoTransaction.newBuilder().setOnReceive(onReceive).build();
    }

    // Helper: pull first value out of the receipt; [] if none.
    private static byte[] extractValueOrEmpty(proto.client.Client.ProtoClientReply reply) {
        if (!reply.hasReceipt()) {
            return new byte[0];
        }
        proto.client.Client.ProtoTransactionReceipt rcpt = reply.getReceipt();

        if (!rcpt.hasResults()) {
            return new byte[0];
        }
        proto.execution.Execution.ProtoTransactionResult tr = rcpt.getResults();

        if (tr.getResultCount() == 0) {
            return new byte[0];
        }
        proto.execution.Execution.ProtoTransactionOpResult op = tr.getResult(0);

        if (op.getValuesCount() == 0) {
            return new byte[0];
        }
        return op.getValues(0).toByteArray();
    }

    private void startCheckerThread(String node) {
        replyLoops.computeIfAbsent(
                node,
                n -> {
                    Thread t =
                            new Thread(
                                    () -> {
                                        try {
                                            this.readyFor(n).acquire();
                                        } catch (InterruptedException e) {
                                            LOG.error(
                                                    "reply loop interrupted for node {}: {}",
                                                    n,
                                                    e.toString());
                                            return;
                                        }
                                        while (running) {
                                            try {
                                                // LOG.info(
                                                //         "permits={}, queued={}, fair={}",
                                                //         maxInFlight.availablePermits(),
                                                //         maxInFlight.getQueueLength(), //
                                                // estimated
                                                //         // number of
                                                //         // waiting
                                                //         // threads
                                                //         maxInFlight.isFair());
                                                // LOG.info("pending size = {}", pending.size());
                                                // LOG.info("pending keys = {}", pending.keySet());
                                                PinnedClient.PinnedMessage msg =
                                                        transport.awaitReply(n);
                                                final proto.client.Client.ProtoClientReply reply =
                                                        proto.client.Client.ProtoClientReply
                                                                .parseFrom(
                                                                        com.google.protobuf
                                                                                .CodedInputStream
                                                                                .newInstance(
                                                                                        msg.buf,
                                                                                        0,
                                                                                        msg.length));

                                                long tag = reply.getClientTag();
                                                CompletableFuture<byte[]> fut = pending.remove(tag);

                                                boolean release = false;
                                                if (fut != null) {
                                                    switch (reply.getReplyCase()) {
                                                        case RECEIPT:
                                                            // fut.complete(
                                                            //         extractValueOrEmpty(reply));
                                                            fut.complete(new byte[0]);
                                                            maxInFlight.release();
                                                            break;
                                                        default:
                                                            maxInFlight.release();
                                                            fut.completeExceptionally(
                                                                    new IOException(
                                                                            "PSL: empty/unknown reply"));
                                                    }
                                                }

                                            } catch (IOException e) {
                                                LOG.warn(
                                                        "awaitReply({}) failed: {}",
                                                        n,
                                                        e.toString());
                                            } catch (Throwable t2) {
                                                LOG.error(
                                                        "reply loop crashed for node {}: {}",
                                                        n,
                                                        t2.toString(),
                                                        t2);
                                                break;
                                            }
                                        }
                                    },
                                    "psl-reply-loop-" + n);
                    t.setDaemon(true);
                    t.start();
                    return t;
                });
    }
}
