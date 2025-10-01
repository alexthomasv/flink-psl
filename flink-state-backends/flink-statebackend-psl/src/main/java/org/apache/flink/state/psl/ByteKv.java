/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements...
 */

package org.apache.flink.state.psl;

import java.io.IOException;

/** Minimal byte[] K/V contract the backend can delegate to. */
public interface ByteKv {
    byte[] get(byte[] key) throws IOException;

    void put(byte[] key, byte[] value) throws IOException;

    void delete(byte[] key) throws IOException;
}
