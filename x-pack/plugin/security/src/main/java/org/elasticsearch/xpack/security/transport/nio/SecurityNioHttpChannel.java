/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.http.nio.NioHttpChannel;

import javax.net.ssl.SSLEngine;
import java.nio.channels.SocketChannel;

public class SecurityNioHttpChannel extends NioHttpChannel {

    private final SSLEngine sslEngine;

    SecurityNioHttpChannel(SocketChannel socketChannel, SSLEngine sslEngine) {
        super(socketChannel);
        this.sslEngine = sslEngine;
    }

    public SSLEngine getSSLEngine() {
        return sslEngine;
    }

    public boolean isTLSEnabled() {
        return  sslEngine != null;
    }
}
