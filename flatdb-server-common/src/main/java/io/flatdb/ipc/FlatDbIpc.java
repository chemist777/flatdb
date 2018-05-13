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
    private final ByteBufferConsumer receiveMethod, sendMethod;

    public FlatDbIpc(String fileNamePrefix, boolean serverMode) {
        try {
            this.serverPipePath = makePipe(fileNamePrefix, true);
            this.serverChannel = FileChannel.open(serverPipePath, serverMode ? StandardOpenOption.READ : StandardOpenOption.WRITE);

            this.clientPipePath = makePipe(fileNamePrefix, false);
            this.clientChannel = FileChannel.open(clientPipePath, serverMode ? StandardOpenOption.WRITE : StandardOpenOption.READ);

            receiveMethod = serverMode ? serverChannel::read : clientChannel::read;
            sendMethod = serverMode ? clientChannel::write : serverChannel::write;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Can't open pipe", e);
        }
    }

    private Path makePipe(String fileNamePrefix, boolean server) throws IOException, InterruptedException {
        Path pipePath = Paths.get(System.getProperty("java.io.tmpdir")).resolve(fileNamePrefix + (server?"server":"client") + ".pipe");
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
            throw new RuntimeException("Can't close pipe", e);
        }
        try {
            clientChannel.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close pipe", e);
        }
    }

    private interface ByteBufferConsumer {
        void accept(ByteBuffer buffer) throws IOException;
    }
}
