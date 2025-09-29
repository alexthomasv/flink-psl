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
import org.apache.flink.runtime.state.KeyGroupRange;

/**
 * Placeholder for a future KVS-backed keyed state backend. Not wired into Flink yet; kept as a file
 * you can evolve.
 */
public final class PslKeyedStateBackend<K> {

    public PslKeyedStateBackend(
            JobID jobId,
            String operatorId,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) {
        // TODO: keep references for later when you implement your backend.
    }

    public void close() {
        // TODO: cleanup when implemented
    }
}
