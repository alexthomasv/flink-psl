package com.psl.utils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Utilities for building an {@link SSLContext} that trusts one or more X.509 certificates provided
 * in PEM format.
 *
 * <p>Typical use:
 *
 * <pre>{@code
 * File rootCa = new File("/path/to/Pft_root_cert.pem");
 * SSLContext ctx = SslUtil.sslContextFromPem(rootCa);
 * // pass ctx into your PinnedClient
 * }</pre>
 *
 * <p>This class is intentionally minimal and does not handle private keys or client authentication.
 * It only constructs a trust store for server authentication.
 */
public final class SslUtil {

    /** Build an SSLContext that trusts the PEM cert(s) in rootCaPem (CA or server cert). */
    public static SSLContext sslContextFromPem(File rootCaPem) throws Exception {
        List<X509Certificate> certs = parsePemCertificates(rootCaPem);

        // In-memory keystore that holds the trusted certs (no password needed)
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType()); // "jks" on most JVMs
        ks.load(null, null);
        int i = 0;
        for (X509Certificate cert : certs) {
            ks.setCertificateEntry("trusted-" + (i++), cert);
        }

        // Trust managers from that keystore
        TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        // TLS context
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    /** Minimal PEM parser for one or more CERTIFICATE blocks. */
    private static List<X509Certificate> parsePemCertificates(File pemFile) throws Exception {
        String pem = readAll(pemFile);
        List<X509Certificate> out = new ArrayList<>();
        CertificateFactory cf = CertificateFactory.getInstance("X.509");

        String begin = "-----BEGIN CERTIFICATE-----";
        String end = "-----END CERTIFICATE-----";
        int pos = 0;
        while (true) {
            int s = pem.indexOf(begin, pos);
            if (s < 0) {
                break;
            }
            int e = pem.indexOf(end, s);
            if (e < 0) {
                throw new IOException("Unclosed CERTIFICATE block in " + pemFile);
            }
            String b64 = pem.substring(s + begin.length(), e).replaceAll("\\s+", "");
            byte[] der = Base64.getDecoder().decode(b64);
            X509Certificate cert =
                    (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(der));
            out.add(cert);
            pos = e + end.length();
        }
        if (out.isEmpty()) {
            throw new IOException("No CERTIFICATE blocks found in " + pemFile);
        }
        return out;
    }

    private static String readAll(File f) throws IOException {
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
}
