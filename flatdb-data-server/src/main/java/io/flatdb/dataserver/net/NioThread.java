package io.flatdb.dataserver.net;

import io.flatdb.dataserver.DataServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Protocol
 *
 * Client request
 * int - size prefix (size itself isn't included)
 * int - request number
 * byte - operation (0 - read, 1 - write)
 * int - partition
 * byte[] - request body
 *
 * Data server response
 * int - size prefix
 * int - request number
 * byte - response type (0 - success, 1 - exception)
 * byte[] - response body
 */
public class NioThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(NioThread.class);
    private static final byte READ_OPERATION = 0;
    private static final byte WRITE_OPERATION = 1;

    private static final byte SUCCESS_RESPONSE = 0;
    private static final byte EXCEPTION_RESPONSE = 1;

    private final DataServer dataServer;
    private final Selector selector;
    private ServerSocketChannel serverSocketChannel;
    public final DirectBufferPool directBufferPool = new DirectBufferPool(1024);
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();

    public NioThread(DataServer dataServer) {
        super("nio");
        this.dataServer = dataServer;
        try {
            this.selector = SelectorProvider.provider().openSelector();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        try {
            while (!isInterrupted()) {
                int keys = selector.select(100);
                if (keys != 0) processSelectedKeys();
                processTasks();
            }
//        } catch (ClosedSelectorException e) {
            //expected exception
        } catch (Exception e) {
            log.error("Unexpected exception on nio thread", e);
        }
    }

    private void processSelectedKeys() throws IOException {
        for (SelectionKey key : selector.selectedKeys()) {
            if (key.channel() == serverSocketChannel && key.isAcceptable()) {
                //it's a connection requests
                SocketChannel channel;
                while ((channel = serverSocketChannel.accept()) != null) {
                    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    //we want to read data from clients
                    channel.register(selector, SelectionKey.OP_READ);
                }
            } else if (key.isReadable()) {
                //we have got some data from a client
                read(key, (SocketChannel) key.channel());
            } else if (!key.isValid()) {
                //connection with a client has been closed
                release(key);
            }
        }
    }

    private void processTasks() {
        Runnable task;
        while ((task = tasks.poll()) != null) {
            task.run();
        }
    }

    private void read(SelectionKey key, SocketChannel channel) throws IOException {
        PendingReadState pendingReadState = (PendingReadState) key.attachment();
        ByteBuffer buffer;
        if (pendingReadState == null) {
            buffer = directBufferPool.get();
        } else {
            buffer = pendingReadState.lastBuffer();
        }

//        read(key, channel, buffer);

        for(;;) {
            int read = channel.read(buffer);
            if (read < 0) {
                //channel is closed
                release(key);
                return;
            } else if (read == 0) {
                break;
            }
            if (buffer.remaining() == 0) {
                //buffer is full
                if (pendingReadState == null) {
                    pendingReadState = new PendingReadState();
                    key.attach(pendingReadState);
                }
                pendingReadState.buffers.add(buffer);
                buffer = directBufferPool.get();
            }
        }

        if (pendingReadState == null) {
            buffer.flip();
            if (buffer.remaining() >= Integer.BYTES) {
                //we can read size
                int size = buffer.getInt();
                if (buffer.remaining() >= size) {
                    //we can read whole request
                    int reqId = buffer.getInt();
                    byte operation = buffer.get();
                    int partition = buffer.getInt();
                    processRequest(channel, reqId, operation, partition, buffer);
                } else {
                    //we can't read whole request

                }
            } else {
                //we can't read size
                pendingReadState = new PendingReadState();
                pendingReadState.buffers.add(buffer);
                key.attach(pendingReadState);
            }
        }
    }

//    private void read(SelectionKey key, SocketChannel channel, ByteBuffer outputBuffer) {
//
//    }

    private void processRequest(SocketChannel channel, int reqId, byte operation, int partition, ByteBuffer request) {
        CompletableFuture<?> future;

        if (operation == READ_OPERATION) {
            future = dataServer.readOperation(partition, request);
        } else if (operation == WRITE_OPERATION) {
            future = dataServer.writeOperation(partition, request);
        } else {
            future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Unsupported operation " + operation));
        }

        future.whenComplete((v, e) -> {
            ByteBuffer response;
            //make response
            if (e == null) {
                int responseLength = v == null ? 0 : ((ByteBuffer) v).remaining();
                //success response
                response = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Byte.BYTES + responseLength);
                response.putInt(response.capacity());
                response.putInt(reqId);
                response.put(SUCCESS_RESPONSE);
                if (responseLength != 0) response.put((ByteBuffer) v);
                response.flip();
            } else {
                //error response
                byte[] stackTrace = stackTrace(e);
                response = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Byte.BYTES + stackTrace.length);
                response.putInt(response.capacity());
                response.putInt(reqId);
                response.put(EXCEPTION_RESPONSE);
                response.put(stackTrace);
                response.flip();
            }
            //send response
            try {
                channel.write(response);
            } catch (IOException e1) {
                throw new RuntimeException(e1); //todo catch it
            }
        });
    }

    private static byte[] stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void release(SelectionKey key) {
        PendingReadState pendingReadState = (PendingReadState) key.attachment();
        if (pendingReadState == null) return;
        for (ByteBuffer buffer : pendingReadState.buffers) {
            directBufferPool.release(buffer);
        }
        key.attach(null);
    }

    public void execute(Runnable task) {
        tasks.add(task);
    }

    @Override
    public synchronized void start() {
        try {
            serverSocketChannel = SelectorProvider.provider().openServerSocketChannel();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverSocketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            //listen on all interfaces
            SocketAddress address = new InetSocketAddress(7707);
            serverSocketChannel.bind(address, 4096);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.start();
    }

    @Override
    public void interrupt() {
        try {
            selector.close();
        } catch (IOException e) {
            log.error("Can't close selector", e);
        }
        super.interrupt();
    }
}
