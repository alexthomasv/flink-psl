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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Blocking KV client that builds ProtoTransaction, wraps it into ProtoClientRequest and
 * ProtoPayload, sends via PinnedClient and waits for ProtoClientReply. Handles TRY_AGAIN and LEADER
 * redirects.
 */
public final class KVSClient {

    private final ClientWorker.PinnedClient client;
    private final AtomicLong tagSeq = new AtomicLong(1L);

    /** Max attempts per call. Tune as you like. */
    private static final int MAX_ATTEMPTS = 64;

    private static final Duration BACKOFF = Duration.ofMillis(200);

    public KVSClient(ClientWorker.PinnedClient client) {
        this.client = client;
    }

    // -----------------------------------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------------------------------

    /** Linearizable write: write(key,value) on crash-commit to leader. */
    public void put(byte[] key, byte[] value) {
        Execution.ProtoTransaction tx = buildWriteCrashCommitTx(key, value);
        // writes go to leader
        sendRoundTrip(tx, /*requireLeader=*/ true);
    }

    /**
     * Read value for key.
     *
     * @param linearizable true -> crash-commit read to leader; false -> on_receive read to any node
     * @return value bytes or null if none present
     */
    public byte[] get(byte[] key, boolean linearizable) {
        final Execution.ProtoTransaction tx =
                linearizable ? buildReadCrashCommitTx(key) : buildReadOnReceiveTx(key);
        final Client.ProtoClientReply reply = sendRoundTrip(tx, /*requireLeader=*/ linearizable);
        if (reply.hasReceipt() && reply.getReceipt().hasResults()) {
            Execution.ProtoTransactionResult tr = reply.getReceipt().getResults();
            if (tr.getResultCount() > 0) {
                Execution.ProtoTransactionOpResult opRes = tr.getResult(0);
                if (opRes.getValuesCount() > 0) {
                    return opRes.getValues(0).toByteArray();
                }
            }
        }
        // Tentative receipt or receipt w/o values -> treat as not found
        return null;
    }

    // -----------------------------------------------------------------------------------------------
    // Round-trip core
    // -----------------------------------------------------------------------------------------------

    private Client.ProtoClientReply sendRoundTrip(
            Execution.ProtoTransaction tx, boolean requireLeader) {

        ClientWorker.ClientConfig cfg = client.getConfigRef().get();
        final String origin = cfg.netConfig.name;
        final long tag = tagSeq.getAndIncrement();

        Client.ProtoClientRequest req =
                Client.ProtoClientRequest.newBuilder()
                        .setTx(tx)
                        .setOrigin(origin)
                        // TODO: add real signature if you wire keystore
                        .setSig(ByteString.copyFrom(new byte[] {0}))
                        .setClientTag(tag)
                        .build();

        Rpc.ProtoPayload payload = Rpc.ProtoPayload.newBuilder().setClientRequest(req).build();
        final byte[] buf = payload.toByteArray();
        final int len = buf.length;

        // local routing view (refresh on redirects/try-again)
        List<String> nodes = new ArrayList<>(cfg.netConfig.nodes.keySet());
        Collections.sort(nodes);

        int leaderIdx = 0; // server corrects us if wrong
        int rrIdx = 0;

        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            if (nodes.isEmpty()) {
                sleep(BACKOFF);
                // refresh from live config (maybe updated by other calls)
                ClientWorker.ClientConfig cur = client.getConfigRef().get();
                nodes = new ArrayList<>(cur.netConfig.nodes.keySet());
                Collections.sort(nodes);
                continue;
            }

            final String target =
                    requireLeader
                            ? nodes.get(Math.floorMod(leaderIdx, nodes.size()))
                            : nodes.get(Math.floorMod(rrIdx++, nodes.size()));

            boolean sent =
                    client.send(
                            target,
                            new ClientWorker.MessageRef(buf, len, ClientWorker.SenderType.ANON));
            if (!sent) {
                if (requireLeader) {
                    leaderIdx = (leaderIdx + 1) % Math.max(nodes.size(), 1);
                }
                sleep(BACKOFF);
                continue;
            }

            Optional<ClientWorker.ReceivedMessage> maybe = client.awaitReply(target);
            if (!maybe.isPresent()) {
                if (requireLeader) {
                    leaderIdx = (leaderIdx + 1) % Math.max(nodes.size(), 1);
                }
                sleep(BACKOFF);
                continue;
            }

            ClientWorker.ReceivedMessage rm = maybe.get();
            final Client.ProtoClientReply reply;
            try {
                CodedInputStream cis = CodedInputStream.newInstance(rm.bytes, 0, rm.length);
                reply = Client.ProtoClientReply.parseFrom(cis);
            } catch (Exception parse) {
                sleep(BACKOFF);
                continue;
            }

            if (reply.hasReceipt() || reply.hasTentativeReceipt()) {
                return reply;
            }

            if (reply.hasTryAgain()) {
                String s = reply.getTryAgain().getSerializedNodeInfos();
                if (s != null && !s.isEmpty()) {
                    applyNodeUpdate(s);
                    ClientWorker.ClientConfig cur = client.getConfigRef().get();
                    nodes = new ArrayList<>(cur.netConfig.nodes.keySet());
                    Collections.sort(nodes);
                }
                sleep(BACKOFF);
                continue;
            }

            if (reply.hasLeader()) {
                String leaderName = reply.getLeader().getName();
                String s = reply.getLeader().getSerializedNodeInfos();
                if (s != null && !s.isEmpty()) {
                    applyNodeUpdate(s);
                }
                ClientWorker.ClientConfig cur = client.getConfigRef().get();
                nodes = new ArrayList<>(cur.netConfig.nodes.keySet());
                Collections.sort(nodes);
                int idx = nodes.indexOf(leaderName);
                if (idx >= 0) {
                    leaderIdx = idx;
                }
                // retry immediately
                continue;
            }

            // REPLY_NOT_SET or unknown -> retry
            sleep(BACKOFF);
        }

        throw new RuntimeException("KVSClient: exceeded max attempts without success");
    }

    // -----------------------------------------------------------------------------------------------
    // Protobuf tx builders
    // -----------------------------------------------------------------------------------------------

    private static Execution.ProtoTransaction buildReadOnReceiveTx(byte[] key) {
        Execution.ProtoTransactionOp read =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.READ)
                        .addOperands(ByteString.copyFrom(key))
                        .build();
        Execution.ProtoTransactionPhase onReceive =
                Execution.ProtoTransactionPhase.newBuilder().addOps(read).build();
        return Execution.ProtoTransaction.newBuilder().setOnReceive(onReceive).build();
    }

    private static Execution.ProtoTransaction buildReadCrashCommitTx(byte[] key) {
        Execution.ProtoTransactionOp read =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.READ)
                        .addOperands(ByteString.copyFrom(key))
                        .build();
        Execution.ProtoTransactionPhase onCrash =
                Execution.ProtoTransactionPhase.newBuilder().addOps(read).build();
        return Execution.ProtoTransaction.newBuilder().setOnCrashCommit(onCrash).build();
    }

    private static Execution.ProtoTransaction buildWriteCrashCommitTx(byte[] key, byte[] value) {
        Execution.ProtoTransactionOp write =
                Execution.ProtoTransactionOp.newBuilder()
                        .setOpType(Execution.ProtoTransactionOpType.WRITE)
                        .addOperands(ByteString.copyFrom(key))
                        .addOperands(ByteString.copyFrom(value))
                        .build();
        Execution.ProtoTransactionPhase onCrash =
                Execution.ProtoTransactionPhase.newBuilder().addOps(write).build();
        return Execution.ProtoTransaction.newBuilder().setOnCrashCommit(onCrash).build();
    }

    // -----------------------------------------------------------------------------------------------
    // Topology helpers
    // -----------------------------------------------------------------------------------------------

    private void applyNodeUpdate(String serializedNodeInfos) {
        try {
            ClientWorker.NodeInfo ni =
                    ClientWorker.NodeInfo.deserialize(
                            serializedNodeInfos.getBytes(StandardCharsets.UTF_8));
            if (ni == null || ni.nodes == null || ni.nodes.isEmpty()) {
                return;
            }
            ClientWorker.ClientConfig oldCfg = client.getConfigRef().get();
            LinkedHashMap<String, ClientWorker.Node> nodes = new LinkedHashMap<>(ni.nodes);
            ClientWorker.NetConfig updated = oldCfg.netConfig.copyWith(nodes);
            ClientWorker.ClientConfig newer = oldCfg.copyWith(updated);
            client.getConfigRef().set(newer);
        } catch (Exception ignore) {
            // keep current topology if parsing fails
        }
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(Math.max(1L, d.toMillis()));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
