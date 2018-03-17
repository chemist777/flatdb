package io.flatdb.client;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class FlatDBClient {
    public FlatDBClient(String dbName) {

    }

    public CompletableFuture<ByteBuffer> readOperation(int partition, ByteBuffer request) {
        //todo
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> writeOperation(ByteBuffer request) {
        //todo
        return CompletableFuture.completedFuture(null);
    }
}
