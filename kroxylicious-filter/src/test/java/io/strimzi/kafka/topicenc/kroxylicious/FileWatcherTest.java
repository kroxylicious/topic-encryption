package io.strimzi.kafka.topicenc.kroxylicious;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;

class FileWatcherTest {

    @Test
    public void testChange(@TempDir Path tempDir) throws Exception {
        Path tempfile = tempDir.resolve("tempfile");
        Files.writeString(tempfile, "ABC");
        CompletableFuture<FileWatcher.Kind> change = new CompletableFuture<>();
        try (FileWatcher watcher = new FileWatcher()) {
            CompletableFuture<FileWatcher.WatchId> watchIdCompletableFuture = watcher.watchFileForCreateOrModify(tempfile, change::complete);
            pollUntil(watcher, () -> watchIdCompletableFuture.getNow(null) != null);
            Files.writeString(tempfile, "DEF", StandardOpenOption.APPEND);
            pollUntil(watcher, () -> FileWatcher.Kind.MODIFIED.equals(change.getNow(null)));
        }
    }

    @Test
    public void testDeregister(@TempDir Path tempDir) throws Exception {
        Path tempfile = tempDir.resolve("tempfile");
        Files.writeString(tempfile, "ABC");
        CompletableFuture<FileWatcher.Kind> change = new CompletableFuture<>();
        try (FileWatcher fileWatcher = new FileWatcher()) {
            CompletableFuture<FileWatcher.WatchId> watchIdCompletableFuture = fileWatcher.watchFileForCreateOrModify(tempfile, change::complete);
            pollUntil(fileWatcher, () -> watchIdCompletableFuture.getNow(null) != null);
            CompletableFuture<Void> unwatch = fileWatcher.unwatch(watchIdCompletableFuture.getNow(null));
            pollUntil(fileWatcher, unwatch::isDone);
            Files.writeString(tempfile, "DEF", StandardOpenOption.APPEND);
            fileWatcher.poll();
            assertFalse(change.isDone());
        }
    }

    // this tests that we reset the WatchKey after each event
    @Test
    public void testMultipleChanges(@TempDir Path tempDir) throws Exception {
        Path tempfile = tempDir.resolve("tempfile");
        Files.writeString(tempfile, "ABC");
        AtomicLong invocations = new AtomicLong();
        try (FileWatcher fileWatcher = new FileWatcher()) {
            CompletableFuture<FileWatcher.WatchId> future = fileWatcher.watchFileForCreateOrModify(tempfile, kind -> {
                if (kind == FileWatcher.Kind.MODIFIED) {
                    invocations.incrementAndGet();
                }
            });
            pollUntil(fileWatcher, () -> future.getNow(null) != null);
            Files.writeString(tempfile, "DEF", StandardOpenOption.APPEND);
            pollUntil(fileWatcher, () -> invocations.get() == 1);
            Files.writeString(tempfile, "HIJ", StandardOpenOption.APPEND);
            pollUntil(fileWatcher, () -> invocations.get() == 2);
        }
    }

    // this tests that we reset the WatchKey after each event
    @Test
    public void testMultipleRecreations(@TempDir Path tempDir) throws Exception {
        Path tempfile = tempDir.resolve("tempfile");
        Files.writeString(tempfile, "ABC");
        AtomicLong invocations = new AtomicLong();
        try (FileWatcher fileWatcher = new FileWatcher()) {
            CompletableFuture<FileWatcher.WatchId> future = fileWatcher.watchFileForCreateOrModify(tempfile, kind -> {
                if (kind == FileWatcher.Kind.CREATED) {
                    invocations.incrementAndGet();
                }
            });
            pollUntil(fileWatcher, () -> future.getNow(null) != null);
            Files.delete(tempfile);
            Files.writeString(tempfile, "DEF");
            pollUntil(fileWatcher, () -> invocations.get() == 1);
            Files.delete(tempfile);
            Files.writeString(tempfile, "HIJ");
            pollUntil(fileWatcher, () -> invocations.get() == 2);
        }
    }

    @Test
    public void testRecreate(@TempDir Path tempDir) throws Exception {
        Path tempfile = tempDir.resolve("tempfile");
        Files.writeString(tempfile, "ABC");
        CompletableFuture<FileWatcher.Kind> change = new CompletableFuture<>();
        try (FileWatcher fileWatcher = new FileWatcher()) {
            CompletableFuture<FileWatcher.WatchId> future = fileWatcher.watchFileForCreateOrModify(tempfile, change::complete);
            pollUntil(fileWatcher, () -> future.getNow(null) != null);
            Files.delete(tempfile);
            Files.writeString(tempfile, "DEF");
            pollUntil(fileWatcher, () -> FileWatcher.Kind.CREATED.equals(change.getNow(null)));
        }
    }

    @Test
    public void testChangeNotifiesMultipleWatchers(@TempDir Path tempDir) throws Exception {
        Path tempfile = tempDir.resolve("tempfile");
        Files.writeString(tempfile, "ABC");
        CompletableFuture<FileWatcher.Kind> change = new CompletableFuture<>();
        CompletableFuture<FileWatcher.Kind> change2 = new CompletableFuture<>();
        try (FileWatcher fileWatcher = new FileWatcher()) {
            CompletableFuture<FileWatcher.WatchId> future1 = fileWatcher.watchFileForCreateOrModify(tempfile, change::complete);
            CompletableFuture<FileWatcher.WatchId> future2 = fileWatcher.watchFileForCreateOrModify(tempfile, change2::complete);
            pollUntil(fileWatcher, () -> future1.getNow(null) != null && future2.getNow(null) != null);

            Files.writeString(tempfile, "DEF", StandardOpenOption.APPEND);
            pollUntil(fileWatcher, () -> FileWatcher.Kind.MODIFIED.equals(change.getNow(null)) && FileWatcher.Kind.MODIFIED.equals(change2.getNow(null)));
        }
    }

    public void pollUntil(FileWatcher watcher, Callable<Boolean> test) {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            watcher.poll();
            return test.call();
        });
    }

}