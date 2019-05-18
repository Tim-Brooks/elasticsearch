/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.FlushReadyWrite;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.Page;
import org.elasticsearch.nio.ReadWriteHandler;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;

public class SSLReadWriteHandlerTests extends ESTestCase {

    public void testPingPong() throws Exception {
        NioSelector nioSelector = mock(NioSelector.class);

        SSLContext sslContext = getSSLContext();

        SSLDriver clientDriver = getDriver(sslContext.createSSLEngine(), true);
        SSLDriver serverDriver = getDriver(sslContext.createSSLEngine(), false);

        FakeHandler clientDelegate = new FakeHandler();
        FakeHandler serverDelegate = new FakeHandler();
        SSLReadWriteHandler clientHandler = new SSLReadWriteHandler(nioSelector, clientDriver, clientDelegate,
            InboundChannelBuffer.allocatingInstance());
        SSLReadWriteHandler serverHandler = new SSLReadWriteHandler(nioSelector, serverDriver, serverDelegate,
            InboundChannelBuffer.allocatingInstance());

        clientHandler.channelRegistered();
        serverHandler.channelRegistered();
    }

    private void write(SSLReadWriteHandler handler1, SSLReadWriteHandler handler2) throws IOException {
        InboundChannelBuffer networkBuffer = InboundChannelBuffer.allocatingInstance();
        while (true) {
            List<FlushOperation> flushOperations = handler1.pollFlushOperations();
            boolean handler1IsWriting = true;
            if (flushOperations.isEmpty()) {
                handler1IsWriting = false;
                flushOperations = handler2.pollFlushOperations();
            }
            if (flushOperations.isEmpty()) {
                return;
            }

            for (FlushOperation flushOperation : flushOperations) {
                networkBuffer.ensureCapacity(networkBuffer.getCapacity());
            }
        }
    }

    private SSLContext getSSLContext() throws Exception {
        String certPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt";
        String keyPath = "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem";
        SSLContext sslContext;
        TrustManager tm = CertParsingUtils.trustManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))));
        KeyManager km = CertParsingUtils.keyManager(CertParsingUtils.readCertificates(Collections.singletonList(getDataPath
            (certPath))), PemUtils.readPrivateKey(getDataPath(keyPath), "testclient"::toCharArray), "testclient".toCharArray());
        sslContext = SSLContext.getInstance(randomFrom("TLSv1.2", "TLSv1.3"));
        sslContext.init(new KeyManager[] { km }, new TrustManager[] { tm }, new SecureRandom());
        return sslContext;
    }

    private SSLDriver getDriver(SSLEngine engine, boolean isClient) {
        return new SSLDriver(engine, (n) -> new Page(ByteBuffer.allocate(n), () -> {}), isClient);
    }

    private static class FakeHandler implements ReadWriteHandler {

        private boolean isClosed = false;
        private ArrayList<FlushOperation> outbound = new ArrayList<>();
        private ArrayList<ByteBuffer[]> inbound = new ArrayList<>();

        @Override
        public void channelRegistered() throws IOException {

        }

        @Override
        public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
            return new FlushReadyWrite(context, (ByteBuffer[]) message, listener);
        }

        @Override
        public List<FlushOperation> writeToBytes(WriteOperation writeOperation) throws IOException {
            return Collections.singletonList((FlushReadyWrite) writeOperation);
        }

        @Override
        public List<FlushOperation> pollFlushOperations() throws IOException {
            ArrayList<FlushOperation> flushOperations = new ArrayList<>(outbound);
            outbound.clear();
            return flushOperations;
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            long readyBytes = channelBuffer.getIndex();
            inbound.add(channelBuffer.sliceBuffersTo(readyBytes));
            channelBuffer.release(readyBytes);
            return (int) readyBytes;
        }

        @Override
        public void initiateProtocolClose() throws IOException {
            isClosed = true;
        }

        @Override
        public boolean isProtocolClosed() {
            return isClosed;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
