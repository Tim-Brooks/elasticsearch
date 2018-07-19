package org.elasticsearch.nio;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface TransportLayer {

    int read(SocketChannel socketChannel) throws IOException;

    void flushChannel(SocketChannel socketChannel) throws IOException;

    void initiateClose();

    boolean shouldClose();

    void close();
}
