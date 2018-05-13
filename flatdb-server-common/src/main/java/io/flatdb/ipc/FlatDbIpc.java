package io.flatdb.ipc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FlatDbIpc {
    public final Path serverPipePath, clientPipePath;
    private final FileChannel serverChannel, clientChannel;
    private final ByteBufferConsumer receiveMethod, sendMethod;

    public FlatDbIpc(Path folderPath, String fileNamePrefix, boolean serverMode) {
        try {
            this.serverPipePath = makePipe(folderPath.resolve(fileNamePrefix + "server.pipe"));
            this.serverChannel = FileChannel.open(serverPipePath, serverMode ? StandardOpenOption.READ : StandardOpenOption.WRITE);

            this.clientPipePath = makePipe(folderPath.resolve(fileNamePrefix + "client.pipe"));
            this.clientChannel = FileChannel.open(clientPipePath, serverMode ? StandardOpenOption.WRITE : StandardOpenOption.READ);

            receiveMethod = serverMode ? serverChannel::read : clientChannel::read;
            sendMethod = serverMode ? clientChannel::write : serverChannel::write;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Can't open pipe", e);
        }
    }

    private static Path makePipe(Path pipePath) throws IOException, InterruptedException {
        if (!Files.exists(pipePath)) {
            Process process = new ProcessBuilder("mkfifo", pipePath.toAbsolutePath().toString())
                    .redirectErrorStream(true)
                    .start();
            int exitCode = process.waitFor();
            //we should check for existence again because file can be created concurrently
            if (exitCode != 0 && !Files.exists(pipePath)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (InputStream is = process.getInputStream()) {
                    byte[] buf = new byte[512];
                    int len;
                    while ((len = is.read(buf)) > 0) {
                        baos.write(buf, 0, len);
                    }
                }
                throw new RuntimeException("Can't mkfifo: " + new String(baos.toByteArray(), StandardCharsets.UTF_8));
            }
        }
        return pipePath;
    }

    public void receive(ByteBuffer buffer) {
        try {
            receiveMethod.accept(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Can't read from pipe", e);
        }
    }

    public void send(ByteBuffer buffer) {
        try {
            sendMethod.accept(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Can't write to pipe", e);
        }
    }

    public void close() {
        try {
            serverChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close server pipe " + serverPipePath, e);
        }
        try {
            clientChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close client pipe " + clientPipePath, e);
        }
    }

    private interface ByteBufferConsumer {
        void accept(ByteBuffer buffer) throws IOException;
    }
}
