package com.falkordb.examples.replica;

import java.util.Objects;

/**
 * Where one FalkorDB node lives and how to authenticate against it.
 *
 * <p>One endpoint describes a single node, never a cluster. A primary and each of its replicas are
 * separate endpoints, because the whole point of read routing is to address them individually.
 *
 * <p>FalkorDB Cloud requires TLS and an ACL user, so {@link #tlsEnabled} defaults to on in
 * {@link #cloud}. A local Docker container normally needs neither, which {@link #local} covers.
 */
public final class Endpoint {

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final boolean tlsEnabled;

    private Endpoint(String host, int port, String username, String password, boolean tlsEnabled) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be in [1, 65535], but was " + port);
        }
        this.host = host.trim();
        this.port = port;
        this.username = username;
        this.password = password;
        this.tlsEnabled = tlsEnabled;
    }

    /** An endpoint with no TLS and no credentials, matching a default local container. */
    public static Endpoint local(String host, int port) {
        return new Endpoint(host, port, null, null, false);
    }

    /** An endpoint with TLS and ACL credentials, matching FalkorDB Cloud. */
    public static Endpoint cloud(String host, int port, String username, String password) {
        return new Endpoint(host, port, username, password, true);
    }

    /** An endpoint with every option stated explicitly. */
    public static Endpoint of(String host, int port, String username, String password, boolean tlsEnabled) {
        return new Endpoint(host, port, username, password, tlsEnabled);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    /** Host and port only. Credentials are deliberately excluded so they cannot reach a log. */
    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Endpoint)) {
            return false;
        }
        Endpoint that = (Endpoint) other;
        return port == that.port && tlsEnabled == that.tlsEnabled && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, tlsEnabled);
    }
}
