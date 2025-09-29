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

import com.psl.utils.KVSClient;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.internal.InternalPriorityQueueSetFactory;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal keyed state backend backed by KVSClient.
 * Supports ValueState only. No timers. Snapshot is a no-op.
 */
public final class PslKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

  private final KVSClient kv;
  private final TypeSerializer<K> keySer;
  private final int totalKeyGroups;
  private final KeyGroupRange keyGroupRange;

  public PslKeyedStateBackend(
      Environment env,
      JobID jobId,
      String operatorId,
      TypeSerializer<K> keySerializer,
      int numberOfKeyGroups,
      KeyGroupRange keyGroupRange,
      KVSClient kvClient) throws IOException {

    super(
        env.getUserCodeClassLoader(),
        env.getExecutionConfig(),
        TtlTimeProvider.DEFAULT,
        keySerializer,
        numberOfKeyGroups,
        keyGroupRange,
        env.getTaskKvStateRegistry());

    this.kv = kvClient;
    this.keySer = keySerializer.duplicate();
    this.totalKeyGroups = numberOfKeyGroups;
    this.keyGroupRange = keyGroupRange;
  }

  // ----------------------------------------------------------------------------------------------
  // ValueState ONLY
  // ----------------------------------------------------------------------------------------------

  @Override
  public <N, T> ValueState<T> createValueState(
      TypeSerializer<N> namespaceSerializer,
      StateDescriptor<T, ValueState<T>> stateDesc) throws Exception {

    final String stateName = stateDesc.getName();
    final TypeSerializer<T> valueSer = stateDesc.getSerializer().duplicate();
    final TypeSerializer<N> nsSer = namespaceSerializer.duplicate();

    return new ValueState<T>() {
      @Override
      public T value() throws IOException {
        try {
          byte[] ck = compositeKey(nsSer, stateName);
          byte[] bytes = kv.get(ck, /*linearizable=*/ true); // you may choose false
          if (bytes == null || bytes.length == 0) {
            return null;
          }
          DataInputDeserializer in = new DataInputDeserializer(bytes);
          return valueSer.deserialize(in);
        } catch (Exception e) {
          throw new IOException("KVS get failed", e);
        }
      }

      @Override
      public void update(T v) throws IOException {
        try {
          byte[] ck = compositeKey(nsSer, stateName);
          if (v == null) {
            // simple delete convention: write 0-byte; add a real DELETE op later
            kv.put(ck, new byte[0]);
          } else {
            DataOutputSerializer out = new DataOutputSerializer(64);
            valueSer.serialize(v, out);
            kv.put(ck, out.getCopyOfBuffer());
          }
        } catch (Exception e) {
          throw new IOException("KVS put failed", e);
        }
      }

      @Override
      public void clear() {
        try {
          byte[] ck = compositeKey(nsSer, stateName);
          kv.put(ck, new byte[0]);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  // ----------------------------------------------------------------------------------------------
  // Timers: NOT SUPPORTED in this minimal backend
  // ----------------------------------------------------------------------------------------------

  @Override
  public InternalPriorityQueueSetFactory getPriorityQueueSetFactory() {
    throw new UnsupportedOperationException("Timers are not supported by PslKeyedStateBackend (prototype).");
  }

  // ----------------------------------------------------------------------------------------------
  // Snapshot / dispose: prototype (no-op snapshot)
  // ----------------------------------------------------------------------------------------------

  @Override
  public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streamFactory,
      CheckpointOptions options) throws Exception {
    return new ImmediateDoneFuture<>(SnapshotResult.empty());
  }

  @Override
  public void dispose() {}

  // ----------------------------------------------------------------------------------------------
  // Composite key: [ keyGroup (2 bytes BE) ][ userKey ][ namespace ][ stateNameLen(4) ][ stateNameBytes ]
  // ----------------------------------------------------------------------------------------------

  private <N> byte[] compositeKey(TypeSerializer<N> nsSer, String stateName) throws IOException {
    final K userKey = getCurrentKey();
    final N ns = (N) getCurrentNamespace();

    DataOutputSerializer out = new DataOutputSerializer(128);

    final int kg = assignToKeyGroup(userKey, totalKeyGroups);
    out.writeShort(kg & 0xFFFF);

    keySer.serialize(userKey, out);
    nsSer.serialize(ns, out);

    byte[] nameBytes = stateName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    out.writeInt(nameBytes.length);
    out.write(nameBytes);

    return out.getCopyOfBuffer();
  }

  // ----------------------------------------------------------------------------------------------
  // Immediate no-op snapshot future
  // ----------------------------------------------------------------------------------------------

  private static final class ImmediateDoneFuture<T> implements RunnableFuture<T> {
    private final T value;
    private final AtomicBoolean done = new AtomicBoolean(false);

    ImmediateDoneFuture(T value) {
      this.value = value;
    }

    @Override
    public void run() {
      done.set(true);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) { return false; }

    @Override
    public boolean isCancelled() { return false; }

    @Override
    public boolean isDone() { return true; }

    @Override
    public T get() { return value; }

    @Override
    public T get(long timeout, java.util.concurrent.TimeUnit unit) { return value; }
  }
}