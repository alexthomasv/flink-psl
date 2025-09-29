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

 import java.nio.charset.StandardCharsets;
 import java.time.Duration;
 import java.util.Objects;
 import java.util.concurrent.CompletableFuture;
 import java.util.concurrent.ExecutorService;
 import java.util.concurrent.TimeUnit;
 
 /**
  * KVSClient wraps a {@link ClientWorker} to provide a simple imperative KV API.
  *
  * <p>It launches the worker (generator + checker tasks) and exposes {@code get}/{@code put}
  * methods. Under the hood, it relies on {@code ClientWorker}'s public async APIs:
  * <ul>
  *   <li>{@code CompletableFuture<Boolean> put(byte[] key, byte[] value)}</li>
  *   <li>{@code CompletableFuture<byte[]> get(byte[] key, boolean linearizable)}</li>
  * </ul>
  *
  * <p>Those methods reuse checker for waiting/decoding, and the existing backpressure,
  * retries, and leader redirects. No listener/callback plumbing is required here.
  */
 public final class KVSClient {
 
     /** The underlying worker handling send/await/retry/redirect. */
     private final ClientWorker<?> worker;
 
     /** Executor used to run the worker tasks. */
     private final ExecutorService exec;
 
     /** Whether reads should be linearizable (Leader + crash/byz-commit) or unlogged (on_receive). */
     private final boolean linearizableReads;
 
     /** Optional default timeout for blocking helpers. */
     private final Duration defaultTimeout;
 
     /** Worker handle (returned by launch), kept for lifecycle management. */
     private ClientWorker.WorkerHandle handle;
 
     /**
      * Construct a KVSClient around an existing, configured {@link ClientWorker}.
      *
      * @param worker the worker (already constructed with config + pinned client + stat channel)
      * @param exec executor service on which the worker will be launched
      * @param linearizableReads true for linearizable reads (Leader + commit), false for unlogged reads (on_receive)
      * @param defaultTimeout default timeout for blocking helpers (can be null; then no default)
      */
     public KVSClient(
             ClientWorker<?> worker,
             ExecutorService exec,
             boolean linearizableReads,
             Duration defaultTimeout) {
         this.worker = Objects.requireNonNull(worker, "worker");
         this.exec = Objects.requireNonNull(exec, "exec");
         this.linearizableReads = linearizableReads;
         this.defaultTimeout = defaultTimeout;
     }
 
     /**
      * Launch the underlying worker (spawns checker + generator tasks).
      * Safe to call once; subsequent calls are no-ops.
      */
     public synchronized void start() {
         if (this.handle != null) {
             return;
         }
         this.handle = worker.launch(exec);
     }
 
     /**
      * Stop / interrupt worker tasks by shutting down the executor.
      * Caller owns the executor; this is a convenience that calls {@code shutdownNow()}.
      */
     public void shutdownNow() {
         exec.shutdownNow();
     }
 
     // --------------------------------------------------------------------------------------------
     // Async KV API (preferred): returns CompletableFuture
     // --------------------------------------------------------------------------------------------
 
     /**
      * Async PUT; returns a future that completes to {@code true} on success.
      * The request is routed to the leader and executed at crash-commit (or byz if you changed it).
      */
     public CompletableFuture<Boolean> put(byte[] key, byte[] value) throws InterruptedException {
         Objects.requireNonNull(key, "key");
         Objects.requireNonNull(value, "value");
         return worker.put(key, value);
     }
 
     /**
      * Async GET; returns a future that completes to the value (or {@code null} if not found).
      * Uses {@link #linearizableReads} to choose routing/phase.
      */
     public CompletableFuture<byte[]> get(byte[] key) throws InterruptedException {
         Objects.requireNonNull(key, "key");
         return worker.get(key, linearizableReads);
     }
 
     // Convenience overloads for String keys/values
     public CompletableFuture<Boolean> put(String key, String value) throws InterruptedException {
         return put(bytes(key), bytes(value));
     }
 
     public CompletableFuture<byte[]> get(String key) throws InterruptedException {
         return get(bytes(key));
     }
 
     // --------------------------------------------------------------------------------------------
     // Blocking helpers (optional): convenience wrappers with timeout
     // --------------------------------------------------------------------------------------------
 
     /**
      * Blocking PUT with timeout.
      *
      * @return true on success
      */
     public boolean putBlocking(byte[] key, byte[] value, long timeout, TimeUnit unit) throws Exception {
         return put(key, value).get(timeout, unit);
     }
 
     /**
      * Blocking GET with timeout.
      *
      * @return value bytes, or {@code null} if not found
      */
     public byte[] getBlocking(byte[] key, long timeout, TimeUnit unit) throws Exception {
         return get(key).get(timeout, unit);
     }
 
     /**
      * Blocking PUT using the client's default timeout.
      */
     public boolean putBlocking(byte[] key, byte[] value) throws Exception {
         ensureDefaultTimeout();
         return putBlocking(key, value, defaultTimeout.toMillis(), TimeUnit.MILLISECONDS);
     }
 
     /**
      * Blocking GET using the client's default timeout.
      */
     public byte[] getBlocking(byte[] key) throws Exception {
         ensureDefaultTimeout();
         return getBlocking(key, defaultTimeout.toMillis(), TimeUnit.MILLISECONDS);
     }
 
     public boolean putBlocking(String key, String value, long timeout, TimeUnit unit) throws Exception {
         return putBlocking(bytes(key), bytes(value), timeout, unit);
     }
 
     public byte[] getBlocking(String key, long timeout, TimeUnit unit) throws Exception {
         return getBlocking(bytes(key), timeout, unit);
     }
 
     public boolean putBlocking(String key, String value) throws Exception {
         return putBlocking(bytes(key), bytes(value));
     }
 
     public byte[] getBlocking(String key) throws Exception {
         return getBlocking(bytes(key));
     }
 
     // --------------------------------------------------------------------------------------------
 
     private static byte[] bytes(String s) {
         return s.getBytes(StandardCharsets.UTF_8);
     }
 
     private void ensureDefaultTimeout() {
         if (defaultTimeout == null) {
             throw new IllegalStateException("No defaultTimeout configured; use the timeout overloads.");
         }
     }
 }