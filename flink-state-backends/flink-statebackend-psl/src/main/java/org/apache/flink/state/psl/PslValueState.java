/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements...
 */

package org.apache.flink.state.psl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * ValueState backed by a byte[] key-value store. The composite storage key is [ keyGroup(2B) ][
 * serialize(currentKey) ][ serialize(namespace) ][ stateNameLen(4B) ][ stateName ].
 */
public final class PslValueState<K, N, SV> implements ValueState<SV>, InternalKvState<K, N, SV> {

    private final PslKeyedStateBackend<K> backend;
    private final ByteKv kv;
    private final String stateName;
    private final TypeSerializer<N> nsSer;
    private final TypeSerializer<SV> valSer;
    private final StateDescriptor<?, SV> desc;

    /** Set by Flink before access; defaults to VoidNamespace for non-windowed state. */
    private N currentNs;

    public PslValueState(
            PslKeyedStateBackend<K> backend,
            ByteKv kv,
            String stateName,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> valueSerializer,
            StateDescriptor<?, SV> descriptor) {

        this.backend = backend;
        this.kv = kv;
        this.stateName = stateName;
        this.nsSer = namespaceSerializer;
        this.valSer = valueSerializer;
        this.desc = descriptor;

        if (nsSer == VoidNamespaceSerializer.INSTANCE) {
            @SuppressWarnings("unchecked")
            N vn = (N) VoidNamespace.INSTANCE;
            this.currentNs = vn;
        }
    }

    // ---------------- ValueState ----------------

    @Override
    public SV value() throws IOException {
        byte[] ck = composeKey();
        byte[] raw = kv.get(ck);
        if (raw == null || raw.length == 0) {
            return null;
        }
        return deserialize(valSer, raw);
    }

    @Override
    public void update(SV v) throws IOException {
        byte[] ck = composeKey();
        if (v == null) {
            kv.delete(ck);
        } else {
            kv.put(ck, serialize(valSer, v));
        }
    }

    @Override
    public void clear() {
        try {
            kv.delete(composeKey());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // --------------- InternalKvState ---------------

    @Override
    public void setCurrentNamespace(N ns) {
        this.currentNs = ns;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return nsSer;
    }

    @Override
    public TypeSerializer<SV> getValueSerializer() {
        return valSer;
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<SV> safeValueSerializer)
            throws Exception {

        final Tuple2<K, N> keyAndNs =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        final int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyAndNs.f0, backend.getKeyContext().getNumberOfKeyGroups());

        final DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeShort((short) keyGroup);
        safeKeySerializer.serialize(keyAndNs.f0, out);
        safeNamespaceSerializer.serialize(keyAndNs.f1, out);
        final byte[] name = stateName.getBytes(StandardCharsets.UTF_8);
        out.writeInt(name.length);
        out.write(name);

        return kv.get(out.getCopyOfBuffer());
    }

    @Override
    public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        // Unsupported for this backend; returning null signals lack of support.
        return null;
    }

    // --------------- helpers ---------------

    /** Compose storage key: [ keyGroup(2B) ][ key ][ namespace ][ nameLen(4B) ][ stateName ]. */
    private byte[] composeKey() throws IOException {
        final K currentKey = backend.getKeyContext().getCurrentKey();
        final int keyGroup = backend.getKeyContext().getCurrentKeyGroupIndex();

        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeShort((short) keyGroup); // key-group (2B, BE)
        backend.getKeySerializer().serialize(currentKey, out); // user key
        nsSer.serialize(currentNs, out); // namespace
        byte[] name = stateName.getBytes(StandardCharsets.UTF_8); // state name
        out.writeInt(name.length);
        out.write(name);

        return out.getCopyOfBuffer();
    }

    private static <T> byte[] serialize(TypeSerializer<T> ser, T v) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        ser.serialize(v, out);
        return out.getCopyOfBuffer();
    }

    private static <T> T deserialize(TypeSerializer<T> ser, byte[] bytes) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        return ser.deserialize(in);
    }
}
