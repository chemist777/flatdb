package io.flatdb.dataserver;

public interface Config {
    Class<? extends DB> dbClass();
    default int dbThreads() {
        return 1;
    }
    default String dbName() {
        return "default";
    }
    Iterable<Integer> partitions();
    boolean stopServerOnJVMShutdown();
}
