package io.flatdb.ipc;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FlatDbIpcTest {
    @Test
    public void test() throws IOException {
        ExecutorService executor1 = Executors.newSingleThreadExecutor();
        ExecutorService executor2 = Executors.newSingleThreadExecutor();
        try {
            Future<?> future1 = executor1.submit(() -> {
                try {
                    FlatDbIpc ipc = new FlatDbIpc(true);
                    try {
                        ByteBuffer buf = ByteBuffer.allocate(10);
                        ipc.receive(buf);
                        buf.flip();
                        assertThat(buf.remaining(), is(Integer.BYTES));
                        assertThat(buf.getInt(), is(1));
                        buf.clear();
                        buf.putInt(2);
                        buf.flip();
                        ipc.send(buf);
                    } finally {
                        ipc.close();
                        Files.deleteIfExists(ipc.serverPipePath);
                        Files.deleteIfExists(ipc.clientPipePath);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    fail();
                }
            });

            Future<?> future2 = executor2.submit(() -> {
                try {
                    FlatDbIpc ipc = new FlatDbIpc(false);
                    try {
                        ByteBuffer buffer = ByteBuffer.allocate(10);
                        buffer.putInt(1);
                        buffer.flip();
                        ipc.send(buffer);
                        buffer.clear();
                        ipc.receive(buffer);
                        buffer.flip();
                        assertThat(buffer.remaining(), is(Integer.BYTES));
                        assertThat(buffer.getInt(), is(2));
                    } finally {
                        ipc.close();
                        Files.deleteIfExists(ipc.serverPipePath);
                        Files.deleteIfExists(ipc.clientPipePath);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    fail();
                }
            });

            try {
                future1.get();
                future2.get();
            } catch (Exception e) {
                //ignore
            }

        } finally {
            executor1.shutdown();
            executor2.shutdown();
        }
    }
}