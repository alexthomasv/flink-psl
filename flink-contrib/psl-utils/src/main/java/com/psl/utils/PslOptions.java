package com.psl.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * PSL-specific configuration keys for initializing the PSL client inside a Flink TaskManager.
 *
 * <p>These options are typically provided via {@code flink-conf.yaml} and read on the TaskManager
 * side (e.g., via {@code GlobalConfiguration.loadConfiguration()} and {@code
 * Configuration#get(ConfigOption)}). They allow you to inject TLS material and connection metadata
 * for the PSL client without hard-coding file paths.
 *
 * <h2>Example (flink-conf.yaml)</h2>
 *
 * <pre>{@code
 * # TLS trust anchor (.pem) for PSL connections
 * psl.ssl.cert: /etc/psl/certs/Pft_root_cert.pem
 *
 * # Ed25519 private key (.pem) for client authentication/signing
 * psl.ed25519.private-key: /etc/psl/keys/client1_signing_privkey.pem
 *
 * # PSL node endpoint
 * psl.node.host: 10.0.3.131
 * psl.node.port: 3001
 * psl.node.sni: node1.pft.org
 * }</pre>
 *
 * <p><b>Thread-safety:</b> this class only declares constants and is thread-safe.
 */
public final class PslOptions {
    public static final ConfigOption<String> PSL_SSL_CERT =
            ConfigOptions.key("psl.ssl.cert").stringType().noDefaultValue();

    public static final ConfigOption<String> PSL_ED25519_KEY =
            ConfigOptions.key("psl.ed25519.private-key").stringType().noDefaultValue();

    public static final ConfigOption<String> PSL_NODE_HOST =
            ConfigOptions.key("psl.node.host").stringType().defaultValue("127.0.0.1");
    public static final ConfigOption<Integer> PSL_NODE_PORT =
            ConfigOptions.key("psl.node.port").intType().defaultValue(3001);

    private PslOptions() {}
}
