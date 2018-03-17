package io.flatdb.dataserver;

public interface Config {
    Class<DB> dbClass();
    String dbName();
    Iterable<Integer> partitions();
    boolean stopServerOnJVMShutdown();
}
