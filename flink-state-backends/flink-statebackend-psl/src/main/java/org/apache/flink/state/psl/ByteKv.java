/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements...
 */

package org.apache.flink.state.psl;

import com.psl.utils.KVSClient;

import java.io.IOException;

/** Adapts your KVSClient to the ByteKv contract used by PslValueState. */
final class KvsByteKvAdapter implements ByteKv {
    private final KVSClient kvs;
    private final boolean linearizableReads;

    KvsByteKvAdapter(KVSClient kvs, boolean linearizableReads) {
        this.kvs = kvs;
        this.linearizableReads = linearizableReads;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        // KVSClient.get returns null when not found
        try {
            return kvs.get(key, linearizableReads);
        } catch (RuntimeException e) {
            // KVSClient throws RuntimeException on exhausted retries; surface as IOException to
            // backend
            throw new IOException("KVS get failed", e);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            // semantics: update(key, value)
            kvs.put(key, value);
        } catch (RuntimeException e) {
            throw new IOException("KVS put failed", e);
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        try {
            // If you add a real delete() in KVSClient, call it here.
            // Until then, use a convention (e.g., empty value means tombstone) agreed with the
            // server:
            kvs.put(key, new byte[0]); // <-- or change to kvs.delete(key) when you implement it
        } catch (RuntimeException e) {
            throw new IOException("KVS delete failed", e);
        }
    }
}

/** Minimal byte[] K/V contract the backend can delegate to. */
public interface ByteKv {
    byte[] get(byte[] key) throws IOException;

    void put(byte[] key, byte[] value) throws IOException;

    void delete(byte[] key) throws IOException;
}
