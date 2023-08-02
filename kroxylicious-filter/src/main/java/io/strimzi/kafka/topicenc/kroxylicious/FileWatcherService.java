package io.strimzi.kafka.topicenc.kroxylicious;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class FileWatcherService implements Closeable, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FileWatcherService.class);
    private final FileWatcher watcher;
    private final ScheduledExecutorService executor;
    private boolean running = true;

    public FileWatcherService() {
        this(Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "topic-encryption-file-watcher")), new FileWatcher());
    }

    public FileWatcherService(ScheduledExecutorService executor, FileWatcher watcher) {
        this.watcher = watcher;
        this.executor = executor;
        this.executor.execute(this);
    }

    @Override
    public void close() {
        logger.info("stopping file watcher work");
        running = false;
    }

    public CompletableFuture<Void> unwatch(FileWatcher.WatchId watchId) {
        return watcher.unwatch(watchId);
    }

    public CompletableFuture<FileWatcher.WatchId> watchFileForCreateOrModify(Path path, Consumer<FileWatcher.Kind> onChange) {
        return watcher.watchFileForCreateOrModify(path, onChange);
    }

    @Override
    public void run() {
        if (running) {
            try {
                watcher.poll();
            } catch (Exception e) {
                logger.error("Error while polling file watcher, continuing to reschedule", e);
            }
            executor.schedule(this, 1, TimeUnit.SECONDS);
        } else {
            logger.info("service was closed, stopping work");
        }
    }
}
