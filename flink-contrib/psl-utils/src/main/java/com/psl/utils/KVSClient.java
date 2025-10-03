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
import com.google.protobuf.CodedInputStream;
import proto.client.Client;
import proto.execution.Execution;
import proto.rpc.Rpc;

import java.io.IOException;
import java.util.Objects;
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

    private final PinnedClient transport;
    private final String defaultNode;
    private final AtomicLong tagSeq = new AtomicLong(1L);

    /**
     * Creates a KV client with a fixed default node to contact.
     *
     * @param transport initialized {@link PinnedClient}
     * @param defaultNode logical node name present in the transport's configuration (nullable if
     *     you plan to pass the node each call)
     */
    public KVSClient(final PinnedClient transport, final String defaultNode) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.defaultNode = defaultNode;
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
    public void put(final byte[] key, final byte[] value) throws IOException {
        ensureDefaultNode();
        put(defaultNode, key, value);
    }

    /**
     * GET using the configured default node.
     *
     * @param key key bytes
     * @param linearizable true for crash-commit (leader) read; false for on-receive (any) read
     * @return value bytes or {@code null} if missing
     * @throws IOException on transport or parse error
     * @throws IllegalStateException if no default node was provided
     */
    public byte[] get(final byte[] key, final boolean linearizable) throws IOException {
        ensureDefaultNode();
        return get(defaultNode, key, linearizable);
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
    public void put(final String node, final byte[] key, final byte[] value) throws IOException {
        final Execution.ProtoTransaction tx = buildWriteCrashCommitTx(key, value);
        final Client.ProtoClientRequest req = buildRequest(tx);
        final byte[] payload =
                Rpc.ProtoPayload.newBuilder().setClientRequest(req).build().toByteArray();
        transport.sendAndAwaitReply(node, new PinnedClient.MessageRef(payload));
    }

    /**
     * Read from a specific node.
     *
     * @param node logical node name
     * @param key key bytes
     * @param linearizable true for crash-commit (leader) read; false for on-receive (any) read
     * @return value bytes or {@code null} if not found / no value in receipt
     * @throws IOException on transport or parse error
     */
    public byte[] get(final String node, final byte[] key, final boolean linearizable)
            throws IOException {
        final Execution.ProtoTransaction tx =
                linearizable ? buildReadCrashCommitTx(key) : buildReadOnReceiveTx(key);
        final Client.ProtoClientRequest req = buildRequest(tx);
        final byte[] payload =
                Rpc.ProtoPayload.newBuilder().setClientRequest(req).build().toByteArray();

        final PinnedClient.PinnedMessage msg =
                transport.sendAndAwaitReply(node, new PinnedClient.MessageRef(payload));

        try {
            final Client.ProtoClientReply reply =
                    Client.ProtoClientReply.parseFrom(
                            CodedInputStream.newInstance(msg.buf, 0, msg.length));

            if (reply.hasReceipt() && reply.getReceipt().hasResults()) {
                final Execution.ProtoTransactionResult tr = reply.getReceipt().getResults();
                if (tr.getResultCount() > 0) {
                    final Execution.ProtoTransactionOpResult opRes = tr.getResult(0);
                    if (opRes.getValuesCount() > 0) {
                        return opRes.getValues(0).toByteArray();
                    }
                }
            }
            return null;
        } catch (final Exception parse) {
            throw new IOException("Failed to parse ProtoClientReply", parse);
        }
    }

    // -------------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------------

    private void ensureDefaultNode() {
        if (defaultNode == null) {
            throw new IllegalStateException(
                    "No default node configured; use put(node,...) / get(node,...)");
        }
    }

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

    private static Execution.ProtoTransaction buildReadCrashCommitTx(final byte[] key) {
        final Execution.ProtoTransactionOp read =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.READ)
                        .addOperands(ByteString.copyFrom(key))
                        .build();
        final Execution.ProtoTransactionPhase onCrash =
                Execution.ProtoTransactionPhase.newBuilder().addOps(read).build();
        return Execution.ProtoTransaction.newBuilder().setOnCrashCommit(onCrash).build();
    }

    private static Execution.ProtoTransaction buildWriteCrashCommitTx(
            final byte[] key, final byte[] value) {
        final Execution.ProtoTransactionOp write =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.WRITE)
                        .addOperands(ByteString.copyFrom(key))
                        .addOperands(ByteString.copyFrom(value))
                        .build();
        final Execution.ProtoTransactionPhase onCrash =
                Execution.ProtoTransactionPhase.newBuilder().addOps(write).build();
        return Execution.ProtoTransaction.newBuilder().setOnCrashCommit(onCrash).build();
    }
}
