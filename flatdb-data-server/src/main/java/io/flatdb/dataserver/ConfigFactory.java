package io.flatdb.dataserver;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Config file syntax:
 * name=default
 * partitions=1,2,3
 */
public class ConfigFactory {
    public static Config create(Properties properties) {
        String className = properties.getProperty("class", "").trim();
        if (className.isEmpty()) {
            throw new RuntimeException("You must specify class name of the DB implementation in config file!");
        }
        Class<DB> dbClass;
        try {
            //noinspection unchecked
            dbClass = (Class<DB>) Class.forName(className);
        } catch (Exception e) {
            throw new RuntimeException("Can't load class "+ className, e);
        }

        String dbName = properties.getProperty("name", "default");
        String partitions = properties.getProperty("partitions", "");
        String[] array = partitions.split(",");
        Set<Integer> partitionsSet = new HashSet<>();
        for (String partition : array) {
            partition = partition.trim();
            if (partition.isEmpty()) continue;
            partitionsSet.add(Integer.parseInt(partition));
        }
        if (partitionsSet.isEmpty()) throw new RuntimeException("You must specify partitions in config file!");

        return new Config() {
            @Override
            public Class<DB> dbClass() {
                return dbClass;
            }

            @Override
            public String dbName() {
                return dbName;
            }

            @Override
            public Iterable<Integer> partitions() {
                return partitionsSet;
            }

            @Override
            public boolean stopServerOnJVMShutdown() {
                return true;
            }
        };
    }
}
