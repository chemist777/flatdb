package io.flatdb.dataserver;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * All methods are called from the single thread executor, which passed to open method.
 */
public interface DB {
    default CompletableFuture<Void> open(String dbName, int partition, Executor executor) {
        return CompletableFuture.completedFuture(null);
    }
    default CompletableFuture<Void> close() {
        return CompletableFuture.completedFuture(null);
    }
    default CompletableFuture<ByteBuffer> readOperation(ByteBuffer request) {
        return CompletableFuture.completedFuture(null);
    }
    default CompletableFuture<Void> writeOperation(ByteBuffer request) {
        return CompletableFuture.completedFuture(null);
    }
}
