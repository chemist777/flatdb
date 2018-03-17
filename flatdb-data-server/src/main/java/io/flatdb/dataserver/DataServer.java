package io.flatdb.dataserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class DataServer {
    private static final Logger log = LoggerFactory.getLogger(DataServer.class);
    private static final Path configPath = Paths.get("server.conf");

    private final Config config;

    //instance of DB for each partition
    private final Map<Integer, DBState> dbInstances = new HashMap<>();

    public DataServer() {
        this(loadConfig());
    }

    public DataServer(Config config) {
        this.config = config;
        this.config.partitions().forEach(partition -> {
            try {
                DB db = config.dbClass().newInstance();
                dbInstances.put(partition, new DBState(db));
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Config loadConfig() {
        Properties properties = new Properties();
        if (Files.exists(configPath))
            try (InputStream in = Files.newInputStream(configPath)) {
                properties.load(in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        else
            throw new RuntimeException("Can't find " + configPath.toAbsolutePath() + " file");
        return ConfigFactory.create(properties);
    }

    public CompletableFuture<Void> start() {
        if (config.stopServerOnJVMShutdown()) Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        CompletableFuture[] futures = new CompletableFuture[dbInstances.size()];
        int i = 0;
        for (Map.Entry<Integer, DBState> entry : dbInstances.entrySet()) {
            int partition = entry.getKey();
            DBState dbState = entry.getValue();
            CompletableFuture<Void> future;
            try {
                future = dbState.db.open(config.dbName(), partition).thenRun(() -> dbState.started = true);
            } catch (Exception e) {
                future = failed(e);
            }
            future = rewordError(future, () -> "Can't start DB for partition " + partition);
            futures[i++] = future;
        }
        return CompletableFuture.allOf(futures);
    }

    public CompletableFuture<Void> stop() {
        CompletableFuture[] futures = new CompletableFuture[dbInstances.size()];
        int i = 0;
        for (Map.Entry<Integer, DBState> entry : dbInstances.entrySet()) {
            int partition = entry.getKey();
            DBState dbState = entry.getValue();
            CompletableFuture<Void> future;
            //we should only stop successfully started DB instances
            if (dbState.started) {
                try {
                    future = dbState.db.close();
                } catch (Exception e) {
                    future = failed(e);
                }
                future = rewordError(future, () -> "Can't stop DB for partition " + partition);
            } else {
                future = CompletableFuture.completedFuture(null);
            }
            futures[i++] = future;
        }
        return CompletableFuture.allOf(futures);
    }

    private static CompletableFuture<Void> failed(Throwable e) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }

    private static CompletableFuture<Void> rewordError(CompletableFuture<Void> future, Supplier<String> newMessage) {
        return future.exceptionally(e -> {
            throw new RuntimeException(newMessage.get(), e);
        });
    }

    /**
     * Starts DataServer from command line.
     * @param args
     */
    public static void main(String[] args) {
        DataServer server = null;
        try {
            server = new DataServer();
            server.start().join();
        } catch (Exception e) {
            log.error("Can't start server", e);
            if (server != null) server.stop().join();
            System.exit(-1);
        }
    }
}
