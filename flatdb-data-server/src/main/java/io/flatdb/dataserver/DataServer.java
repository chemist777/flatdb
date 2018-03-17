package io.flatdb.dataserver;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class DataServer<T extends DB> {
    private final Path configPath = Paths.get("server.conf");

    private final Config config;

    //instance of DB for each partition
    private final Map<Integer, DB> dbInstances = new HashMap<>();

    public DataServer(Class<T> dbClass) {
        this.config = loadConfig();

        this.config.partitions.forEach(partition -> {
            try {
                DB db = dbClass.newInstance();
                dbInstances.put(partition, db);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Config loadConfig() {
        Properties properties = new Properties();
        if (Files.exists(configPath))
            try (InputStream in = Files.newInputStream(configPath)) {
                properties.load(in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        else
            throw new RuntimeException("Can't find " + configPath + " file");
        return new Config(properties);
    }

    public CompletableFuture<Void> start() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        CompletableFuture[] futures = new CompletableFuture[dbInstances.size()];
        int i = 0;
        for (Map.Entry<Integer, DB> entry : dbInstances.entrySet()) {
            int partition = entry.getKey();
            DB db = entry.getValue();
            futures[i++] = db.open(config.dbName, partition);
        }
        return CompletableFuture.allOf(futures);
    }

    public CompletableFuture<Void> stop() {
        CompletableFuture[] futures = new CompletableFuture[dbInstances.size()];
        int i = 0;
        for (DB db : dbInstances.values()) {
            futures[i++] = db.close();
        }
        return CompletableFuture.allOf(futures);
    }

    /**
     * Starts DataServer from command line.
     * @param args first argument is the name of the DB implementation class
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length == 0) {
            System.err.println("You should pass DB implementation class name in the first argument.");
            System.exit(-1);
            return;
        }
        //noinspection unchecked
        DataServer server = new DataServer(Class.forName(args[0]));
        server.start().join();
    }
}
