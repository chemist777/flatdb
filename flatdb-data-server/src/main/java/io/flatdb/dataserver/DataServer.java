package io.flatdb.dataserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class DataServer {
    private static final Logger log = LoggerFactory.getLogger(DataServer.class);
    private static final Path configPath = Paths.get("server.conf");

    private final Config config;
    private final ExecutorService[] dbExecutors;

    //instance of DB for each partition
    private final Map<Integer, DBContext> dbInstances = new HashMap<>();

    public DataServer() {
        this(loadConfig());
    }

    public DataServer(Config config) {
        this.config = config;
        this.dbExecutors = new ExecutorService[config.dbThreads()];
        for(int i=0, size=dbExecutors.length; i<size; i++) {
            dbExecutors[i] = Executors.newSingleThreadExecutor();
        }
        int executorIndex = 0;
        for (int partition : this.config.partitions()) {
            try {
                DB db = config.dbClass().newInstance();
                dbInstances.put(partition, new DBContext(db, dbExecutors[executorIndex++]));
                executorIndex %= dbExecutors.length;
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
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
        for (Map.Entry<Integer, DBContext> entry : dbInstances.entrySet()) {
            int partition = entry.getKey();
            DBContext dbContext = entry.getValue();
            CompletableFuture<Void> future;
            try {
                future = CompletableFuture.supplyAsync(() -> dbContext.db.open(config.dbName(), partition, dbContext.executor), dbContext.executor)
                        .thenCompose(f -> f)
                        .thenRun(() -> dbContext.started = true);
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
        for (Map.Entry<Integer, DBContext> entry : dbInstances.entrySet()) {
            int partition = entry.getKey();
            DBContext dbContext = entry.getValue();
            CompletableFuture<Void> future;
            //we should only stop successfully started DB instances
            if (dbContext.started) {
                try {
                    future = CompletableFuture.supplyAsync(dbContext.db::close, dbContext.executor)
                            .thenCompose(f -> f)
                            .thenRun(() -> dbContext.started = false);
                } catch (Exception e) {
                    future = failed(e);
                }
                future = rewordError(future, () -> "Can't stop DB for partition " + partition);
            } else {
                future = CompletableFuture.completedFuture(null);
            }
            futures[i++] = future;
        }
        return CompletableFuture.allOf(futures).whenComplete((v, e) -> {
            for (ExecutorService dbExecutor : dbExecutors) {
                dbExecutor.shutdown();
            }
        });
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

    CompletableFuture<ByteBuffer> readOperation(int partition, ByteBuffer request) {
        DBContext context = dbInstances.get(partition);
        return CompletableFuture.supplyAsync(() -> context.db.readOperation(request), context.executor)
                .thenCompose(f -> f);
    }

    CompletableFuture<Void> writeOperation(int partition, ByteBuffer request) {
        DBContext context = dbInstances.get(partition);
        return CompletableFuture.supplyAsync(() -> context.db.writeOperation(request), context.executor)
                .thenCompose(f -> f);
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
