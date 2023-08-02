package io.strimzi.kafka.topicenc.kroxylicious;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class FileWatcherServiceTest {
    @Spy
    private FakeClock clock;
    @Spy
    private FakeScheduledExecutorService executor;
    @Mock
    FileWatcher watcher;

    @Test
    public void testRescheduledOnSuccess() {
        new FileWatcherService(executor, watcher);
        assertEquals(1, executor.pending());
        executor.run();
        Mockito.verify(watcher).poll();
        assertEquals(1, executor.pending());
        clock.elapse(Duration.of(1, ChronoUnit.SECONDS));
        executor.run();
        Mockito.verify(watcher, times(2)).poll();
        assertEquals(1, executor.pending());
    }

    @Test
    public void testRescheduleDuration() {
        new FileWatcherService(executor, watcher);
        assertEquals(1, executor.pending());
        executor.run();
        Mockito.verify(watcher).poll();
        assertEquals(1, executor.pending());
        clock.elapse(Duration.of(999, ChronoUnit.MILLIS));
        executor.run();
        Mockito.verify(watcher, times(1)).poll();
        clock.elapse(Duration.of(1, ChronoUnit.MILLIS));
        executor.run();
        Mockito.verify(watcher, times(2)).poll();
        assertEquals(1, executor.pending());
    }

    @Test
    public void testCloseStopsWorkAndStopsRescheduling() {
        FileWatcherService fileWatcherService = new FileWatcherService(executor, watcher);
        assertEquals(1, executor.pending());
        fileWatcherService.close();
        executor.run();
        Mockito.verify(watcher, never()).poll();
        assertEquals(0, executor.pending());
    }

    @Test
    public void testOtherPollExceptionKeepsRescheduling() {
        new FileWatcherService(executor, watcher);
        assertEquals(1, executor.pending());
        Mockito.doThrow(new RuntimeException("failed to poll! should be non-fatal")).when(watcher).poll();
        executor.run();
        Mockito.verify(watcher, times(1)).poll();
        assertEquals(1, executor.pending());
    }

    static abstract class FakeClock extends Clock {
        private Instant now = Instant.ofEpochMilli(0);

        @Override
        public Instant instant() {
            return now;
        }

        void elapse(Duration duration) {
            now = now.plus(duration);
        }
    }

    record Job(Runnable runnable, long delay, TemporalUnit unit, Instant created) {

        public boolean ready(Instant now) {
            return now.compareTo(created.plus(delay, unit)) >= 0;
        }

        public void run() {
            runnable.run();
        }
    }

    abstract class FakeScheduledExecutorService
            implements ScheduledExecutorService {
        private List<Job> jobs = new ArrayList<>();

        @Override
        public ScheduledFuture<?> schedule(
                @NotNull Runnable command, long delay, TimeUnit unit) {
            jobs.add(new Job(command, delay, unit.toChronoUnit(), clock.instant()));
            return Mockito.mock(ScheduledFuture.class);
        }

        @Override
        public void execute(@NotNull Runnable command) {
            jobs.add(new Job(command, 0, ChronoUnit.MILLIS, clock.instant()));
        }

        void run() {
            Instant now = clock.instant();
            List<Job> ready = jobs.stream().filter(job -> job.ready(now)).toList();
            jobs = jobs.stream()
                    .filter(job -> !job.ready(now))
                    .collect(toCollection(ArrayList::new));
            ready.forEach(Job::run);
        }

        public int pending() {
            return jobs.size();
        }
    }

}