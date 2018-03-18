package io.flatdb.dataserver.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PendingReadState {
    public final List<ByteBuffer> buffers = new ArrayList<>();

    public ByteBuffer lastBuffer() {
        return buffers.get(buffers.size());
    }
}
