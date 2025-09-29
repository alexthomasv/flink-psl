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

package org.apache.flink.state.psl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory; // (not used yet)
import org.apache.flink.runtime.state.memory.DefaultOperatorStateBackendBuilder;

import com.psl.utils.ClientWorker;
import com.psl.utils.KVSClient;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * Entry point: builds a KVSClient and returns a keyed state backend backed by it.
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
      org.apache.flink.runtime.state.KeyGroupRange keyGroupRange,
      TaskKvStateRegistry kvStateRegistry) throws IOException {

    // --- Build a minimal ClientConfig for your transport ---
    final String origin = env.getTaskInfo().getTaskName() + "-" + env.getTaskInfo().getIndexOfThisSubtask();

    // TODO: populate from configuration (flink-conf, env, job params)
    LinkedHashMap<String, ClientWorker.Node> nodes = new LinkedHashMap<>();
    // e.g., nodes.put("node-a", new ClientWorker.Node("node-a"));

    ClientWorker.NetConfig net = new ClientWorker.NetConfig(origin, nodes);
    ClientWorker.WorkloadConfig dummy = new ClientWorker.WorkloadConfig(0, 0); // unused by KVSClient
    ClientWorker.ClientConfig cfg = new ClientWorker.ClientConfig(dummy, net);
    ClientWorker.PinnedClient pinned = new ClientWorker.PinnedClient(cfg);

    KVSClient kv = new KVSClient(pinned);
    kv.setLinearizableReads(linearizableReads);

    return new PslKeyedStateBackend<>(
        env,
        jobId,
        operatorIdentifier,
        keySerializer,
        numberOfKeyGroups,
        keyGroupRange,
        kv);
  }

  @Override
  public OperatorStateBackend createOperatorStateBackend(
      Environment env, String operatorIdentifier) throws IOException {
    // prototype: keep operator state in heap backend
    return new DefaultOperatorStateBackendBuilder(
        env.getUserCodeClassLoader(),
        env.getExecutionConfig()).build();
  }
}