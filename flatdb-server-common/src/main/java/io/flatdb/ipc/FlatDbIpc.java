package io.flatdb.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FlatDbIpc {
    private final FileChannel serverChannel, clientChannel;
    public final Path serverPipePath, clientPipePath;
    private boolean serverMode;

    public FlatDbIpc(boolean serverMode) {
        this.serverMode = serverMode;
        try {
            this.serverPipePath = makePipe(true);
            this.serverChannel = FileChannel.open(serverPipePath, serverMode ? StandardOpenOption.READ : StandardOpenOption.WRITE);

            this.clientPipePath = makePipe(false);
            this.clientChannel = FileChannel.open(clientPipePath, serverMode ? StandardOpenOption.WRITE : StandardOpenOption.READ);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Can't open pipe", e);
        }
    }

    private Path makePipe(boolean server) throws IOException, InterruptedException {
        Path pipePath = Paths.get(System.getProperty("java.io.tmpdir")).resolve("flatdb-" + (server?"server":"client") + ".pipe");
        if (!Files.exists(pipePath)) {
            Process process = new ProcessBuilder("mkfifo", pipePath.toAbsolutePath().toString())
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            int exitCode = process.waitFor();
            //we should check for existence again because file can be created concurrently
            if (exitCode != 0 && !Files.exists(pipePath)) {
                throw new RuntimeException("Can't mkfifo, exit code " + exitCode);
            }
        }
        return pipePath;
    }

    public void receive(ByteBuffer buffer) {
        try {
            (serverMode ? serverChannel : clientChannel).read(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Can't read from pipe", e);
        }
    }

    public void send(ByteBuffer buffer) {
        try {
            (serverMode ? clientChannel : serverChannel).write(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Can't write to pipe", e);
        }
    }

    public void close() {
        try {
            serverChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close pipe", e);
        }
        try {
            clientChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close pipe", e);
        }
    }
}
