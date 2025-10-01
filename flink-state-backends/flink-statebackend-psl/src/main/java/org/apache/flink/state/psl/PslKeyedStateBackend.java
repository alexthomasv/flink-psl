/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.flink.state.psl;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import com.psl.utils.KVSClient;

import javax.annotation.Nonnull;

import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/**
 * Placeholder for a future KVS-backed keyed state backend. Not wired into Flink yet; kept as a file
 * you can evolve.
 */
public final class PslKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private final KVSClient kvs;
    private final boolean linearizableReads;

    public PslKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext,
            KVSClient kvs,
            boolean linearizableReads) {
        // TODO: keep references for later when you implement your backend.
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);
        this.kvs = kvs;
        this.linearizableReads = linearizableReads;
    }

    public void close() {
        // TODO: cleanup when implemented
    }

    @Override
    public int numKeyValueStateEntries() {
        // TODO: return the actual number of KV state entries when implemented
        return 0;
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions)
            throws Exception {
        // TODO: implement real snapshot logic for this backend
        return DoneFuture.of(SnapshotResult.empty());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // no-op for placeholder backend
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException("Not implemented for placeholder backend");
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return Stream.empty();
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return Stream.empty();
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        throw new UnsupportedOperationException("Not implemented for placeholder backend");
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {

        if (stateDesc instanceof ValueStateDescriptor<?>) {
            final String stateName = stateDesc.getName();
            final TypeSerializer<SV> valSer =
                    ((ValueStateDescriptor<SV>) stateDesc).getSerializer();

            // Adapt your KVSClient to ByteKv here:
            ByteKv kv = new KvsByteKvAdapter(this.kvs, this.linearizableReads);

            PslValueState<K, N, SV> vs =
                    new PslValueState<>(
                            this, kv, stateName, namespaceSerializer, valSer, stateDesc);

            return (IS) vs;
        }

        throw new UnsupportedOperationException("Only ValueState supported in prototype.");
    }
}
