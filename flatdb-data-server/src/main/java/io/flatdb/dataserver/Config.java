package io.flatdb.dataserver;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Config file syntax:
 * name=default
 * partitions=1,2,3
 */
public class Config {
    public final String dbName;
    public final Iterable<Integer> partitions;

    public Config(Properties properties) {
        this.dbName = properties.getProperty("name", "default");
        String partitions = properties.getProperty("partitions", "");
        String[] array = partitions.split(",");
        Set<Integer> partitionsSet = new HashSet<>();
        for (String partition : array) {
            partition = partition.trim();
            if (partition.isEmpty()) continue;
            partitionsSet.add(Integer.parseInt(partition));
        }
        if (partitionsSet.isEmpty()) throw new RuntimeException("You must specify partitions in config file!");
        this.partitions = partitionsSet;
    }
}
