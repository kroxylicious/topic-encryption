package io.strimzi.kafka.topicenc.kroxylicious;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Watches for modifications and creations of specified files, invoking a user
 * supplied callback on modification. Note that the callback will be executed in the thread
 * polling for File changes so work should be done reasonably quickly to not block other
 * listeners.
 */
public class FileWatcher implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);
    private final AtomicLong watchIds = new AtomicLong(0);

    @Override
    public void close() {
        try {
            logger.info("closing file watcher");
            service.close();
            logger.info("WatchService closed");
        } catch (Exception e) {
            logger.error("failed to close watch service", e);
        }
    }

    public enum Kind {
        CREATED, MODIFIED
    }

    record WatchFileRequest(Path file, Path directory, Consumer<Kind> onEvent,
                            CompletableFuture<WatchId> registered) {

    }

    public record WatchId(long id) {
    }

    public record UnwatchFileRequest(WatchId id, CompletableFuture<Void> complete) {
    }

    public CompletableFuture<Void> unwatch(WatchId watchId) {
        CompletableFuture<Void> complete = new CompletableFuture<>();
        boolean add = unwatchFileRequestRequests.add(new UnwatchFileRequest(watchId, complete));
        if (!add) {
            throw new RuntimeException("Failed to add deregistration to queue");
        } else {
            return complete;
        }
    }

    record RegisteredFileWatch(WatchId id, WatchFileRequest watch, WatchKey key) {

        public boolean matches(WatchKey poll, Path relativePath) {
            return key.equals(poll) && watch.directory.resolve(relativePath).equals(watch.file);
        }
    }

    ArrayBlockingQueue<WatchFileRequest> watchRequests = new ArrayBlockingQueue<>(100);
    ArrayBlockingQueue<UnwatchFileRequest> unwatchFileRequestRequests = new ArrayBlockingQueue<>(100);
    WatchService service;
    List<RegisteredFileWatch> fileWatchList = new ArrayList<>();

    private static final Map<WatchEvent.Kind<?>, Kind> interested = Map.of(ENTRY_CREATE, Kind.CREATED, ENTRY_MODIFY, Kind.MODIFIED);

    public FileWatcher() {
        try {
            service = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void poll() {
        drainWatchFileRequests();
        drainUnwatchFileRequests();
        pollForFileSystemChanges();
    }

    private void drainUnwatchFileRequests() {
        UnwatchFileRequest toRemove;
        while ((toRemove = unwatchFileRequestRequests.poll()) != null) {
            try {
                WatchId finalToRemove = toRemove.id;
                fileWatchList.removeIf(registeredFileWatch -> registeredFileWatch.id.equals(finalToRemove));
                logger.info("Removed watcher for {}", toRemove);
                toRemove.complete.complete(null);
            } catch (Exception e) {
                logger.error("Failed to remove watcher for {}", toRemove);
                toRemove.complete.completeExceptionally(e);
            }
        }
    }

    public CompletableFuture<WatchId> watchFileForCreateOrModify(Path path, Consumer<Kind> onChange) {
        if (!Files.isRegularFile(path)) {
            logger.error("attempted to watch a path that is not a file {}", path);
            throw new IllegalArgumentException(path + " is not a file");
        }
        Path absolutePath = path.toAbsolutePath();
        CompletableFuture<WatchId> registered = new CompletableFuture<>();
        boolean add = watchRequests.add(new WatchFileRequest(absolutePath, absolutePath.getParent(), onChange, registered));
        if (add) {
            logger.info("added registration to queue for {}", path);
        } else {
            logger.error("failed to add registration to queue for {}", path);
            throw new RuntimeException(path + " is not a file");
        }
        return registered;
    }

    private void pollForFileSystemChanges() {
        if (fileWatchList.isEmpty()) {
            return;
        }
        WatchKey poll = service.poll();
        if (poll != null) {
            List<WatchEvent<?>> watchEvents = poll.pollEvents();
            for (WatchEvent<?> watchEvent : watchEvents) {
                if (interested.containsKey(watchEvent.kind())) {
                    Path path = (Path) watchEvent.context();
                    for (RegisteredFileWatch fileWatch : fileWatchList) {
                        if (fileWatch.matches(poll, path)) {
                            Kind kind = interested.get(watchEvent.kind());
                            logger.info("file {} detected for {}", kind, fileWatch.watch.file);
                            try {
                                fileWatch.watch.onEvent.accept(kind);
                            } catch (Exception e) {
                                logger.error("exception thrown while invoking users event handler", e);
                            }
                        }
                    }
                }
            }
            poll.reset();
        }
    }

    private void drainWatchFileRequests() {
        WatchFileRequest watch;
        while ((watch = watchRequests.poll()) != null) {
            try {
                WatchKey key = watch.directory.register(service, ENTRY_CREATE, ENTRY_MODIFY);
                WatchId id = new WatchId(watchIds.incrementAndGet());
                fileWatchList.add(new RegisteredFileWatch(id, watch, key));
                logger.info("Registered watcher for {}", watch.file);
                watch.registered.complete(id);
            } catch (Exception e) {
                logger.error("Failed to register watcher for {}", watch.file);
                watch.registered.completeExceptionally(e);
            }
        }
    }

}
