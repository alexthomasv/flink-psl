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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.io.IOException;
import java.util.Collection;

/**
 * Minimal PSL StateBackend scaffold for Flink 1.16.3.
 *
 * <p>Methods are present so the module compiles, but intentionally throw
 * UnsupportedOperationException until you implement your backend.
 */
public final class PslStateBackend implements StateBackend {

    private final boolean linearizableReads;

    public PslStateBackend(boolean linearizableReads) {
        this.linearizableReads = linearizableReads;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobId,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            Collection<KeyedStateHandle> restoredStateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {

        throw new UnsupportedOperationException("PSL keyed state backend not implemented yet.");
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            Collection<org.apache.flink.runtime.state.OperatorStateHandle> restoredStateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {

        throw new UnsupportedOperationException("PSL operator state backend not implemented yet.");
    }
}
