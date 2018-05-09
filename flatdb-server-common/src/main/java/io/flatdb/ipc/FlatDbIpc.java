package io.flatdb.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FlatDbIpc {
    private final FileChannel channel;

    public FlatDbIpc(Path pipePath) {
        try {
            if (!Files.exists(pipePath)) {
                Process process = new ProcessBuilder("mkfifo", pipePath.toAbsolutePath().toString()).inheritIO().start();
                int exitCode = process.waitFor();
                //we should check for existence again because file can be created concurrently
                if (exitCode != 0 && !Files.exists(pipePath)) {
                    throw new RuntimeException("Can't mkfifo, exit code " + exitCode);
                }
            }
            this.channel = FileChannel.open(pipePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Can't open pipe", e);
        }
    }

    public void receive(ByteBuffer buffer) {
        try {
            channel.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Can't read from pipe", e);
        }
    }

    public void send(ByteBuffer buffer) {
        try {
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Can't write to pipe", e);
        }
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException("Can't close pipe", e);
        }
    }
}
