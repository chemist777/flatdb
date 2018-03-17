package io.flatdb.dataserver;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface DB {
    CompletableFuture<Void> open(String dbName, int partition);
    CompletableFuture<ByteBuffer> readOperation(ByteBuffer request);
    CompletableFuture<Void> writeOperation(ByteBuffer request);
    CompletableFuture<Void> close();
}
