package dev.rainsolo.scheduler.scheduler;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persists the last fire time (epoch millis) of every job into a Properties file.
 * A new instance reloads the file, which is how a restarted scheduler recovers
 * its schedule. A null path gives an in-memory store for deterministic tests.
 */
public final class LastFireStore {

    private final Path path;
    private final ConcurrentHashMap<String, Long> values = new ConcurrentHashMap<>();

    public LastFireStore(Path path) {
        this.path = path;
        load();
    }

    /** Creates a store that keeps values in memory and never touches the disk. */
    public static LastFireStore inMemory() {
        return new LastFireStore(null);
    }

    /** Reloads values from disk, discarding the current cache. */
    public synchronized void load() {
        values.clear();
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            Properties properties = new Properties();
            properties.load(reader);
            for (String key : properties.stringPropertyNames()) {
                values.put(key, Long.parseLong(properties.getProperty(key).trim()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load last-fire store from " + path, e);
        }
    }

    /** Returns the last fire time, or null when the job has never fired. */
    public Long lastFire(String jobName) {
        return values.get(jobName);
    }

    /** Updates the last fire time and immediately persists the snapshot. */
    public synchronized void update(String jobName, long fireTimeMillis) {
        values.put(jobName, fireTimeMillis);
        flush();
    }

    /** Writes the current state to disk; a no-op for an in-memory store. */
    public synchronized void snapshot() {
        flush();
    }

    private void flush() {
        if (path == null) {
            return;
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Properties properties = new Properties();
            values.forEach((key, value) -> properties.setProperty(key, Long.toString(value)));
            try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                properties.store(writer, "distributed-scheduler last fire times");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist last-fire store to " + path, e);
        }
    }

    public Path path() {
        return path;
    }

    public int size() {
        return values.size();
    }
}
