package org.grv.audit;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AuditLogger {

    private static final String POISON_PILL = new String("<<stop-audit-logger>>");

    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(); // unbounded: log() never blocks
    private final Path file;
    private Thread writerThread;

    public AuditLogger(Path file) {
        this.file = file;
    }

    public synchronized void start() {
        if (writerThread != null) {
            throw new IllegalStateException("audit logger already started");
        }
        writerThread = new Thread(() -> writeLoop(), "audit-logger");
        writerThread.start();
    }

    private void writeLoop() {
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            while (true) {
                String line = queue.take();
                if (line == POISON_PILL) {
                    break;
                }
                writer.write(line);
                writer.newLine();
                writer.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.err.println("Audit logger stopped, cannot write " + file.toAbsolutePath() + ": " + e);
        }
    }

    public void stop(){
        queue.offer(POISON_PILL);
        try {
            writerThread.join(); // wait until the writer has drained the queue and closed the file
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void log(String line) {
        queue.offer(line);
    }
}
