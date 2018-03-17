package io.flatdb.dataserver;

import java.util.concurrent.Executor;

class DBContext {
    public final DB db;
    public final Executor executor;
    public volatile boolean started;

    DBContext(DB db, Executor executor) {
        this.db = db;
        this.executor = executor;
    }
}
