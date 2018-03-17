package io.flatdb.dataserver;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DataServerTest {
    @Test
    public void test() {
        Config config = new Config() {
            @Override
            public Class<? extends DB> dbClass() {
                return ListDB.class;
            }

            @Override
            public String dbName() {
                return "test";
            }

            @Override
            public Iterable<Integer> partitions() {
                return Arrays.asList(1, 2, 3);
            }

            @Override
            public boolean stopServerOnJVMShutdown() {
                return true;
            }
        };

        DataServer dataServer = new DataServer(config);
        dataServer.start().join();
        //add element to list
        dataServer.writeOperation(1, ByteBuffer.wrap("123".getBytes())).join();
        //get element by index
        ByteBuffer request = ByteBuffer.allocate(Integer.BYTES).putInt(0);
        request.flip();
        ByteBuffer result = dataServer.readOperation(1, request).join();
        assertThat(result.array(), is("123".getBytes()));
        dataServer.stop().join();
    }

    public static class ListDB implements DB {
        private final List<String> lines = new ArrayList<>();

        @Override
        public CompletableFuture<Void> writeOperation(ByteBuffer request) {
            //add line
            lines.add(new String(request.array()));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ByteBuffer> readOperation(ByteBuffer request) {
            //get line with requested index
            int lineIndex = request.getInt();
            return CompletableFuture.completedFuture(ByteBuffer.wrap(lines.get(lineIndex).getBytes()));
        }
    }
}