package io.flatdb.dataserver.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DirectBufferPool {
    private final int bufferSizeBytes;
    private final List<ByteBuffer> pool = new ArrayList<>();

    public DirectBufferPool(int bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
    }

    public ByteBuffer get() {
        if (pool.isEmpty()) {
            return ByteBuffer.allocateDirect(bufferSizeBytes);
        } else {
            return pool.remove(pool.size() - 1);
        }
    }

    public void release(ByteBuffer buffer) {
        buffer.clear();
        pool.add(buffer);
    }
}
