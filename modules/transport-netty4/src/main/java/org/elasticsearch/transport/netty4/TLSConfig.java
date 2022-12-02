/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.common.ssl.SslConfiguration;

import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;

public record TLSConfig(SslConfiguration sslConfiguration, EngineProvider engineProvider) {

    public boolean isTLSEnabled() {
        return sslConfiguration != null;
    }

    public SSLEngine createServerSSLEngine() {
        assert isTLSEnabled();
        SSLEngine sslEngine = engineProvider.create(sslConfiguration, null, -1);
        sslEngine.setUseClientMode(false);
        return sslEngine;
    }

    public SSLEngine createClientSSLEngine(final InetSocketAddress remoteAddress) {
        assert isTLSEnabled();
        final SSLEngine sslEngine;
        if (sslConfiguration.verificationMode().isHostnameVerificationEnabled()) {
            // we create the socket based on the name given. don't reverse DNS
            sslEngine = engineProvider.create(sslConfiguration, remoteAddress.getHostString(), remoteAddress.getPort());
        } else {
            sslEngine = engineProvider.create(sslConfiguration, null, -1);
        }
        sslEngine.setUseClientMode(true);
        return sslEngine;
    }

    public static TLSConfig noTLS() {
        return new TLSConfig(null, null);
    }

    @FunctionalInterface
    public interface EngineProvider {

        SSLEngine create(SslConfiguration configuration, String host, int port);
    }
}
