/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.transport.nio.NioTcpChannel;

import javax.net.ssl.SSLEngine;
import java.nio.channels.SocketChannel;

public class SecurityNioTcpChannel extends NioTcpChannel {

    private final SSLEngine sslEngine;

    SecurityNioTcpChannel(boolean isServer, String profile, SocketChannel socketChannel, SSLEngine sslEngine) {
        super(isServer, profile, socketChannel);
        this.sslEngine = sslEngine;
    }

    public SSLEngine getSSLEngine() {
        return sslEngine;
    }

    public boolean isTLSEnabled() {
        return  sslEngine != null;
    }
}
