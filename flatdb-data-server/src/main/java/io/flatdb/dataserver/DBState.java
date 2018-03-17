package io.flatdb.dataserver;

class DBState {
    public final DB db;
    public volatile boolean started;

    DBState(DB db) {
        this.db = db;
    }
}
