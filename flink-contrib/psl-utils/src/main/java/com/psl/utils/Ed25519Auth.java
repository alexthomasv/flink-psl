package com.psl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proto.auth.Auth;

import javax.net.ssl.SSLSocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Client-side handshake that matches the Rust flow. 1) read 4-byte nonce (big-endian) from TLS
 * socket 2) payload = nonce(4 bytes) || name(UTF-8) 3) Ed25519 sign(payload) 4) send len (u32 BE) +
 * ProtoHandshakeResponse { name, signature, is_reply_channel, client_sub_id }
 */
public final class Ed25519Auth implements PinnedClient.Auth {

    private static final Logger LOG = LoggerFactory.getLogger(Ed25519Auth.class);
    private final String clientName; // must match what the server has in its keylist
    private final String clientSubId; // whatever you were using in Rust (as u64 -> string here)
    private final PrivateKey ed25519Key; // PKCS#8 Ed25519 private key

    public Ed25519Auth(String clientName, String clientSubId, File pkcs8PemPrivateKey)
            throws Exception {
        this.clientName = clientName;
        this.clientSubId = clientSubId;
        this.ed25519Key = loadPkcs8Ed25519FromPem(pkcs8PemPrivateKey);
        ensureBouncyCastle(); // Java 8 needs BC for Ed25519
    }

    @Override
    public void handshakeClient(
            PinnedClient client, SSLSocket socket, boolean fullDuplexMainChannel, String unused)
            throws IOException {
        // 1) read 4-byte nonce (big-endian)
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        int nonce = in.readInt(); // big-endian
        LOG.info("Ed25519Auth: nonce: {}", nonce);
        // 2) payload = nonce(4 bytes BE) || name(UTF-8)
        byte[] payload = buildPayload(nonce, clientName);

        // 3) Ed25519 sign(payload)
        byte[] signature;
        try {
            Signature sig = Signature.getInstance("Ed25519", "BC");
            LOG.info("Ed25519Auth: Signature: {}", sig);
            sig.initSign(ed25519Key);
            sig.update(payload);
            signature = sig.sign();
        } catch (GeneralSecurityException e) {
            throw new IOException("Ed25519 signing failed", e);
        }

        // 4) Compose proto and send length-prefix + bytes
        // Determine is_reply_channel exactly like Rust expects.
        // Our Auth receives "fullDuplexMainChannel" == true *only* for the main (send) socket in
        // full-duplex mode.
        // Rust wants "is_reply_channel". So:
        boolean isReplyChannel = client.isFullDuplex() && !fullDuplexMainChannel;

        Auth.ProtoHandshakeResponse resp =
                Auth.ProtoHandshakeResponse.newBuilder()
                        .setName(clientName)
                        .setSignature(com.google.protobuf.ByteString.copyFrom(signature))
                        .setIsReplyChannel(isReplyChannel)
                        .setClientSubId(
                                Long.parseLong(
                                        clientSubId)) // Rust sends u64; here we pass the same value
                        // as a
                        // string
                        .build();

        byte[] bytes = resp.toByteArray();
        LOG.info("Ed25519Auth: bytes: {}", bytes);
        DataOutputStream out =
                new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        out.writeInt(bytes.length); // u32 big-endian length
        out.write(bytes);
        out.flush();
        LOG.info("Ed25519Auth: flushed");
    }

    /* ------------------------ helpers ------------------------ */

    private static byte[] buildPayload(int nonce, String name) {
        ByteBuffer buf = ByteBuffer.allocate(4 + name.length());
        buf.putInt(nonce);
        buf.put(name.getBytes(StandardCharsets.UTF_8));
        return buf.array();
    }

    /** Load a PKCS#8 Ed25519 private key from a PEM file. */
    private static PrivateKey loadPkcs8Ed25519FromPem(File pemFile) throws Exception {
        String pem = readAscii(pemFile);
        String begin = "-----BEGIN PRIVATE KEY-----";
        String end = "-----END PRIVATE KEY-----";
        int s = pem.indexOf(begin);
        int e = pem.indexOf(end);
        if (s < 0 || e < 0) {
            throw new IOException("PKCS#8 PRIVATE KEY PEM block not found: " + pemFile);
        }
        String b64 = pem.substring(s + begin.length(), e).replaceAll("\\s+", "");
        byte[] der = Base64.getDecoder().decode(b64);

        ensureBouncyCastle();
        KeyFactory kf = KeyFactory.getInstance("Ed25519", "BC");
        return kf.generatePrivate(new PKCS8EncodedKeySpec(der));
    }

    private static String readAscii(File f) throws IOException {
        try (InputStream in = new FileInputStream(f)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            int r;
            while ((r = in.read(buf)) >= 0) {
                bos.write(buf, 0, r);
            }
            return bos.toString(StandardCharsets.US_ASCII.name());
        }
    }

    private static void ensureBouncyCastle() {
        if (Security.getProvider("BC") == null) {
            try {
                Class<?> bc = Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");
                Security.addProvider((Provider) bc.getDeclaredConstructor().newInstance());
                LOG.info("BouncyCastle provider added");
            } catch (Throwable t) {
                // If you see this, add BouncyCastle to your deps:
                // <dependency>
                //   <groupId>org.bouncycastle</groupId>
                //   <artifactId>bcprov-jdk15on</artifactId>
                //   <version>1.70</version>
                // </dependency>
                throw new IllegalStateException(
                        "BouncyCastle provider not available (needed for Ed25519 on Java 8)", t);
            }
        }
    }
}
