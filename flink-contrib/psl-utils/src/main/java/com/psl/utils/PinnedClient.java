package com.psl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TLS client that sends and receives length-prefixed frames over per-peer connections.
 *
 * <p>Framing format:
 *
 * <pre>
 *   u32 length (big-endian) + length bytes of payload
 * </pre>
 *
 * <h3>Duplex model</h3>
 *
 * <p>If {@link Config#fullDuplex} is {@code true}, the client maintains two TLS sockets per peer:
 *
 * <ul>
 *   <li><b>send socket</b> under the key {@code name}
 *   <li><b>reply socket</b> under the key {@code name + ":reply"}
 * </ul>
 *
 * <p>Replies are read from the reply socket; otherwise the same socket is used for send and reply.
 *
 * <h3>Authentication hook</h3>
 *
 * <p>If {@link Config#doAuth} is {@code true}, an {@link Auth} callback is invoked after the TLS
 * handshake to perform an application-level handshake.
 *
 * <h3>Threading</h3>
 *
 * <p>Methods are thread-safe at the connection level (synchronized on the socket wrapper for I/O).
 * The client lazily reconnects on demand and drops broken sockets from the map.
 */
public final class PinnedClient {

    private static final Logger LOG = LoggerFactory.getLogger(PinnedClient.class);

    // ----------------------------- Config & Model -----------------------------

    /** Immutable configuration for {@link PinnedClient}. */
    public static final class Config {
        /**
         * If true, use separate sockets for send and reply ({@code name} and {@code name:reply}).
         */
        public final boolean fullDuplex;
        /**
         * If true, invoke {@link Auth#handshakeClient(PinnedClient, SSLSocket, boolean, String)}
         * after TLS.
         */
        public final boolean doAuth;
        /** Application-specific client sub-id forwarded to the auth hook. */
        public final int clientSubId;
        /** Peer topology (names to host/port/domain). */
        public final NetConfig netConfig;
        /** TCP connect timeout. */
        public final Duration connectTimeout = Duration.ofSeconds(5);

        /**
         * @param fullDuplex whether to use dedicated reply sockets
         * @param doAuth whether to run the auth hook after TLS
         * @param clientSubId application-defined sub-id presented to the auth hook
         * @param netConfig peer mapping for connections
         */
        public Config(boolean fullDuplex, boolean doAuth, int clientSubId, NetConfig netConfig) {
            this.fullDuplex = fullDuplex;
            this.doAuth = doAuth;
            this.clientSubId = clientSubId;
            this.netConfig = netConfig;
        }
    }

    /** Mapping of peer names to {@link Node} socket endpoints. */
    public static final class NetConfig {
        /** name -> node endpoint. */
        public final Map<String, Node> nodes;

        /** @param nodes mapping of logical name to endpoint */
        public NetConfig(Map<String, Node> nodes) {
            this.nodes = nodes;
        }
    }

    /** A single socket endpoint (address + SNI domain). */
    public static final class Node {
        /** IPv4/IPv6 hostname or literal. */
        public final String addrHost;
        /** TCP port. */
        public final int addrPort;
        /** SNI / certificate hostname to request/verify. */
        public final String domain;

        /**
         * @param addrHost host (DNS or IP)
         * @param addrPort port
         * @param domain SNI hostname / server certificate identity
         */
        public Node(String addrHost, int addrPort, String domain) {
            this.addrHost = addrHost;
            this.addrPort = addrPort;
            this.domain = domain;
        }
    }

    /** Zero-copy reference to a payload to send. */
    public static final class MessageRef {
        private final byte[] buf;
        private final int len;

        /** @param buf payload bytes (entire buffer will be sent) */
        public MessageRef(byte[] buf) {
            this(buf, buf.length);
        }

        /**
         * @param buf payload buffer
         * @param len number of bytes from the buffer to send
         */
        public MessageRef(byte[] buf, int len) {
            this.buf = buf;
            this.len = len;
        }

        /** @return backing buffer (not copied) */
        public byte[] bytes() {
            return buf;
        }

        /** @return number of valid bytes to send from {@link #bytes()} */
        public int len() {
            return len;
        }
    }

    /** Immutable view of a received framed message. */
    public static final class PinnedMessage {
        /** Backing buffer (may be larger than {@link #length}). */
        public final byte[] buf;
        /** Valid bytes in {@link #buf}. */
        public final int length;
        /** Who this message is attributed to (for higher layers). */
        public final SenderType senderType;

        /**
         * @param buf backing buffer (not defensively copied)
         * @param length valid length within buffer
         * @param senderType attribution for higher layers
         */
        public PinnedMessage(byte[] buf, int length, SenderType senderType) {
            this.buf = buf;
            this.length = length;
            this.senderType = senderType;
        }

        /** @return a {@link MessageRef} referencing the valid portion of {@link #buf}. */
        public MessageRef asRef() {
            return new MessageRef(buf, length);
        }
    }

    /** High-level attribution of a message sender. Extend as needed. */
    public enum SenderType {
        /** Message from an authenticated peer/channel. */
        Auth,
        /** Message from an unauthenticated peer/channel. */
        Anon
    }

    // ----------------------------- TLS Socket Wrapper -----------------------------

    /**
     * TLS socket wrapper.
     *
     * <ul>
     *   <li>Buffered writes with explicit {@link #flushWriteBuffer()}
     *   <li>Framed reads via {@link #getNextFrame(byte[])}
     *   <li>Best-effort nonblocking check via {@link #hasBufferedBytes()}
     * </ul>
     */
    private static final class PinnedTlsSocket {
        private final SSLSocket socket;
        private final DataInputStream in;
        private final BufferedOutputStream out;
        private volatile int bufferedReadable = 0;

        PinnedTlsSocket(SSLSocket socket) throws IOException {
            this.socket = socket;
            this.socket.setTcpNoDelay(true);
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        /** Writes a big-endian 32-bit length prefix to the buffered output. */
        synchronized void writeU32Buffered(int v) throws IOException {
            out.write((v >>> 24) & 0xFF);
            out.write((v >>> 16) & 0xFF);
            out.write((v >>> 8) & 0xFF);
            out.write(v & 0xFF);
        }

        /** Writes {@code len} bytes from {@code b} to the buffered output. */
        synchronized void writeAllBuffered(byte[] b, int len) throws IOException {
            out.write(b, 0, len);
        }

        /** Flushes the buffered output so data is actually transmitted. */
        synchronized void flushWriteBuffer() throws IOException {
            out.flush();
        }

        /**
         * Reads the next framed message (blocking).
         *
         * @param target target buffer; must be large enough
         * @return number of payload bytes copied into {@code target}; {@code 0} if EOS
         * @throws EOFException if the frame header is truncated
         * @throws IOException on I/O errors
         */
        synchronized int getNextFrame(byte[] target) throws IOException {
            // LOG.info("before getNextFrame: readU32();");
            int len = readU32();
            // LOG.info("getNextFrame: len: {}", len);
            if (len <= 0) {
                return 0;
            }
            if (len > target.length) {
                throw new EOFException(
                        "Frame too large for target buffer: " + len + " > " + target.length);
            }
            in.readFully(target, 0, len);
            bufferedReadable = Math.max(0, in.available());
            return len;
        }

        /** @return {@code true} if there are bytes already buffered in the input stream. */
        synchronized boolean hasBufferedBytes() throws IOException {
            bufferedReadable = Math.max(bufferedReadable, in.available());
            return bufferedReadable > 0;
        }

        private int readU32() throws IOException {
            int b1 = in.read();
            if (b1 < 0) {
                return 0;
            }
            int b2 = in.read(), b3 = in.read(), b4 = in.read();
            if ((b2 | b3 | b4) < 0) {
                throw new EOFException("Unexpected EOF in length prefix");
            }
            return ((b1 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 8) | (b4 & 0xFF);
        }

        /** Closes the underlying TLS socket, ignoring I/O errors. */
        synchronized void shutdown() {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    // ----------------------------- Auth hook -----------------------------

    /** Application-level handshake invoked after TLS completes, if enabled. */
    public interface Auth {
        /**
         * @param client the owning {@link PinnedClient}
         * @param socket the TLS socket (already handshaken)
         * @param fullDuplex {@code true} if this is the main (send) channel in full-duplex mode
         * @param clientSubId application-specific sub-id
         * @throws IOException on handshake failure
         */
        void handshakeClient(
                PinnedClient client, SSLSocket socket, boolean fullDuplex, int clientSubId)
                throws IOException;
    }

    // ----------------------------- PinnedClient core -----------------------------

    private final Config cfg;
    private final SSLContext sslContext;
    private final Auth auth; // nullable if doAuth=false

    /** Connection map: {@code name} (and optionally {@code name:reply}) -> socket. */
    private final ConcurrentHashMap<String, PinnedTlsSocket> sockMap = new ConcurrentHashMap<>();

    /**
     * Creates a new client.
     *
     * @param cfg immutable client configuration
     * @param sslContext initialized TLS context with an appropriate trust store
     * @param auth optional auth hook (ignored if {@link Config#doAuth} is false)
     */
    public PinnedClient(Config cfg, SSLContext sslContext, Auth auth) {
        this.cfg = cfg;
        this.sslContext = sslContext;
        this.auth = auth;
    }

    private static String replyName(PinnedClient c, String name) {
        return c.cfg.fullDuplex ? (name + ":reply") : name;
    }

    // ----------------------------- Connect / Get Socket -----------------------------

    /**
     * Connects a single TLS socket for the given peer.
     *
     * @param name peer name from {@link NetConfig#nodes}
     * @param isReplyChannel whether this connection is the reply channel in full-duplex mode
     * @return new connected, handshaken TLS socket
     * @throws IOException on connect/TLS/auth errors
     */
    private PinnedTlsSocket connectOne(String name, boolean isReplyChannel) throws IOException {
        Node n = cfg.netConfig.nodes.get(name);
        if (n == null) {
            throw new IOException("AddrNotAvailable for node: " + name);
        }

        SSLSocketFactory fac = sslContext.getSocketFactory();
        Socket plain = new Socket();
        try {
            plain.connect(
                    new InetSocketAddress(n.addrHost, n.addrPort),
                    (int) cfg.connectTimeout.toMillis());
            plain.setTcpNoDelay(true);

            SSLSocket ssl = (SSLSocket) fac.createSocket(plain, n.addrHost, n.addrPort, true);
            SSLParameters params = ssl.getSSLParameters();
            params.setServerNames(Collections.singletonList(new SNIHostName(n.domain)));
            ssl.setSSLParameters(params);

            ssl.startHandshake();

            if (cfg.doAuth && auth != null) {
                // auth flag is true for the main SEND channel in full-duplex mode
                boolean isMainSendChannel = cfg.fullDuplex && !isReplyChannel;
                auth.handshakeClient(this, ssl, isMainSendChannel, cfg.clientSubId);
            }

            return new PinnedTlsSocket(ssl);
        } catch (IOException e) {
            try {
                plain.close();
            } catch (IOException ignored) {
            }
            throw e;
        }
    }

    /**
     * Ensures entries for {@code name} (and {@code name:reply} if needed) exist in {@link
     * #sockMap}.
     *
     * @param name peer name
     * @return the send socket (under {@code name})
     * @throws IOException on connect/TLS/auth errors
     */
    private synchronized PinnedTlsSocket connect(String name) throws IOException {
        if (cfg.fullDuplex) {
            PinnedTlsSocket main = connectOne(name, false);
            PinnedTlsSocket reply = connectOne(name, true);
            sockMap.put(name, main);
            sockMap.put(replyName(this, name), reply);
            return main;
        } else {
            PinnedTlsSocket single = connectOne(name, false);
            sockMap.put(name, single);
            return single;
        }
    }

    /** @return send socket for {@code name}, reconnecting if necessary */
    private PinnedTlsSocket getSock(String name) throws IOException {
        PinnedTlsSocket s = sockMap.get(name);
        if (s != null) {
            return s;
        }
        return connect(name);
    }

    /**
     * @return reply socket for {@code name} (or send socket if not full-duplex), reconnecting if
     *     necessary
     */
    private PinnedTlsSocket getReplySock(String name) throws IOException {
        String key = cfg.fullDuplex ? replyName(this, name) : name;
        PinnedTlsSocket s = sockMap.get(key);
        if (s != null) {
            return s;
        }
        connect(name); // creates both entries if full-duplex
        s = sockMap.get(key);
        if (s == null) {
            throw new IOException("Failed to establish reply socket for " + name);
        }
        return s;
    }

    // ----------------------------- Low-level buffered send -----------------------------

    private void sendRawSize(String name, PinnedTlsSocket sock, int size) throws IOException {
        try {
            sock.writeU32Buffered(size);
        } catch (IOException e) {
            handleSendError(name, sock, e);
        }
    }

    private void sendRawBytes(String name, PinnedTlsSocket sock, byte[] buf, int len)
            throws IOException {
        try {
            sock.writeAllBuffered(buf, len);
        } catch (IOException e) {
            handleSendError(name, sock, e);
        }
    }

    private void handleSendError(String name, PinnedTlsSocket sock, IOException e)
            throws IOException {
        try {
            sock.shutdown();
        } catch (Exception ignored) {
        }
        sockMap.remove(name);
        sockMap.remove(replyName(this, name));
        throw e;
    }

    // ----------------------------- Public API (mirrors Rust) -----------------------------

    /**
     * Sends one framed message and flushes immediately (blocking).
     *
     * @param name peer name
     * @param data payload reference
     * @throws IOException on I/O errors (connection is dropped and will be re-established next
     *     call)
     */
    public void send(String name, MessageRef data) throws IOException {
        PinnedTlsSocket sock = getSock(name);
        int len = data.len();
        Instant t0 = Instant.now();
        sendRawSize(name, sock, len);
        long szMicros = Duration.between(t0, Instant.now()).toNanos() / 1000;
        sendRawBytes(name, sock, data.bytes(), len);
        long totalMicros = Duration.between(t0, Instant.now()).toNanos() / 1000;
        // Optionally log szMicros / totalMicros here.
        synchronized (sock) {
            sock.flushWriteBuffer();
        }
    }

    /**
     * Sends one framed message but does <b>not</b> flush. Call {@link #forceFlush(String)} later.
     *
     * @param name peer name
     * @param data payload reference
     * @throws IOException on I/O errors
     */
    public void sendBuffered(String name, MessageRef data) throws IOException {
        PinnedTlsSocket sock = getSock(name);
        int len = data.len();
        sendRawSize(name, sock, len);
        sendRawBytes(name, sock, data.bytes(), len);
    }

    /**
     * Forces a flush on the send socket for {@code name}.
     *
     * @param name peer name
     * @throws IOException on I/O errors
     */
    public void forceFlush(String name) throws IOException {
        PinnedTlsSocket sock = getSock(name);
        synchronized (sock) {
            sock.flushWriteBuffer();
        }
    }

    /**
     * Sends one framed message and waits for a single framed reply (blocking).
     *
     * @param name peer name
     * @param data payload
     * @return the received message (buffer reused per call; copy if you need to retain)
     * @throws IOException on I/O errors or EOS
     */
    public PinnedMessage sendAndAwaitReply(String name, MessageRef data) throws IOException {
        PinnedTlsSocket sendSock = getSock(name);
        int len = data.len();
        Instant t0 = Instant.now();
        sendRawSize(name, sendSock, len);
        sendRawBytes(name, sendSock, data.bytes(), len);
        synchronized (sendSock) {
            sendSock.flushWriteBuffer();
        }

        PinnedTlsSocket replySock = getReplySock(name);
        byte[] resp = new byte[256];
        int sz;
        synchronized (replySock) {
            sz = replySock.getNextFrame(resp);
        }
        if (sz == 0) {
            LOG.info("PinnedClient: sendAndAwaitReply: socket probably closed!");
            throw new EOFException("socket probably closed!");
        }
        return new PinnedMessage(resp, sz, SenderType.Auth);
    }

    /**
     * Waits for the next framed reply from {@code name} (blocking).
     *
     * @param name peer name
     * @return received message
     * @throws IOException on I/O errors or EOS
     */
    public PinnedMessage awaitReply(String name) throws IOException {
        PinnedTlsSocket replySock = getReplySock(name);
        byte[] resp = new byte[4096];
        int sz;
        synchronized (replySock) {
            sz = replySock.getNextFrame(resp);
        }
        if (sz == 0) {
            throw new EOFException("socket probably closed!");
        }
        return new PinnedMessage(resp, sz, SenderType.Auth);
    }

    /**
     * Attempts to read a reply without blocking.
     *
     * @param name peer name
     * @return {@link Optional#empty()} if no buffered bytes are available; otherwise the message
     * @throws IOException on I/O errors or EOS
     */
    public Optional<PinnedMessage> tryAwaitReply(String name) throws IOException {
        PinnedTlsSocket replySock = getReplySock(name);
        synchronized (replySock) {
            if (!replySock.hasBufferedBytes()) {
                return Optional.empty();
            }
            byte[] resp = new byte[4096];
            int sz = replySock.getNextFrame(resp);
            if (sz == 0) {
                throw new EOFException("socket probably closed!");
            }
            return Optional.of(new PinnedMessage(resp, sz, SenderType.Auth));
        }
    }

    /**
     * Retries {@link #send(String, MessageRef)} up to {@code maxRetries} times.
     *
     * @param name peer name
     * @param data payload
     * @param maxRetries number of attempts (must be &gt;=1)
     * @throws IOException if all attempts fail
     */
    public void reliableSend(String name, MessageRef data, int maxRetries) throws IOException {
        IOException last = null;
        int i = Math.max(1, maxRetries);
        while (i-- > 0) {
            try {
                send(name, data);
                return;
            } catch (IOException e) {
                last = e;
            }
        }
        throw (last != null ? last : new IOException("reliableSend failed"));
    }

    /**
     * Sends the same payload to a list of peers sequentially.
     *
     * @param names peers to send to (order preserved)
     * @param data payload
     * @param minSuccess minimum number of successful sends required
     * @throws IOException if fewer than {@code minSuccess} sends succeed
     */
    public void broadcast(List<String> names, MessageRef data, int minSuccess) throws IOException {
        int ok = 0;
        IOException last = null;
        for (String n : names) {
            try {
                send(n, data);
                ok++;
            } catch (IOException e) {
                last = e;
            }
        }
        if (ok < minSuccess) {
            throw (last != null ? last : new IOException("broadcast failed; ok=" + ok));
        }
    }

    /**
     * Drops (and closes) the sockets for a single peer. Next call will reconnect lazily.
     *
     * @param name peer name
     */
    public void dropConnection(String name) {
        PinnedTlsSocket a = sockMap.remove(name);
        PinnedTlsSocket b = sockMap.remove(replyName(this, name));
        if (a != null) {
            a.shutdown();
        }
        if (b != null) {
            b.shutdown();
        }
    }

    /** Drops (and closes) all sockets. Next call will reconnect lazily. */
    public void dropAllConnections() {
        for (String k : new ArrayList<>(sockMap.keySet())) {
            PinnedTlsSocket s = sockMap.remove(k);
            if (s != null) {
                s.shutdown();
            }
        }
    }

    // ----------------------------- Utility: example SSLContext -----------------------------

    /**
     * Builds an {@link SSLContext} that trusts either:
     *
     * <ul>
     *   <li>the provided JKS trust store, if non-null; or
     *   <li>the default system trust store, otherwise.
     * </ul>
     *
     * @param trustStoreJks path to a JKS trust store file (nullable)
     * @param password JKS password (nullable if {@code trustStoreJks} is null)
     * @return initialized TLS context
     * @throws Exception on load/init failures
     */
    public static SSLContext buildSslContext(File trustStoreJks, char[] password) throws Exception {
        TrustManagerFactory tmf;
        if (trustStoreJks != null) {
            KeyStore ks = KeyStore.getInstance("JKS");
            try (InputStream in = new FileInputStream(trustStoreJks)) {
                ks.load(in, password);
            }
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
        } else {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
        }
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    public boolean isFullDuplex() {
        return cfg.fullDuplex;
    }
}
