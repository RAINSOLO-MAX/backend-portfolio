package dev.rainsolo.minirpc.registry;

import dev.rainsolo.minirpc.common.ServiceMeta;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * File-backed registry so provider and consumer can run in separate JVMs.
 * Each line is {@code host:port|weight|lastSeenMillis}; writes go through a temp file
 * and an atomic move so a reader never observes a half-written registry.
 */
public final class FileServiceRegistry implements ServiceRegistry {

    private final Path directory;
    private final long ttlMillis;

    public FileServiceRegistry(Path directory) {
        this(directory, 30_000L);
    }

    public FileServiceRegistry(Path directory, long ttlMillis) {
        this.directory = directory;
        this.ttlMillis = ttlMillis;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create registry directory " + directory, e);
        }
    }

    private Path fileFor(String serviceName) {
        String safe = serviceName.replaceAll("[^a-zA-Z0-9_.-]", "_");
        return directory.resolve(safe + ".registry");
    }

    private record Entry(String endpoint, int weight, long lastSeen) {
    }

    private Map<String, Entry> read(Path file) throws IOException {
        Map<String, Entry> entries = new LinkedHashMap<>();
        if (!Files.exists(file)) {
            return entries;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.split("\\|");
            if (parts.length == 3) {
                try {
                    entries.put(parts[0], new Entry(parts[0], Integer.parseInt(parts[1]), Long.parseLong(parts[2])));
                } catch (NumberFormatException ignored) {
                    // Skip a corrupted line instead of failing the whole lookup.
                }
            }
        }
        return entries;
    }

    private void write(Path file, Map<String, Entry> entries) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (Entry entry : entries.values()) {
            builder.append(entry.endpoint()).append('|').append(entry.weight())
                    .append('|').append(entry.lastSeen()).append('\n');
        }
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(temp, builder.toString(), StandardCharsets.UTF_8);
        try {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public synchronized void register(ServiceMeta meta) {
        renew(meta);
    }

    @Override
    public synchronized void heartbeat(ServiceMeta meta) {
        renew(meta);
    }

    private void renew(ServiceMeta meta) {
        Path file = fileFor(meta.getServiceName());
        try {
            Map<String, Entry> entries = read(file);
            entries.put(meta.endpoint(), new Entry(meta.endpoint(), meta.getWeight(), System.currentTimeMillis()));
            write(file, entries);
        } catch (IOException e) {
            throw new IllegalStateException("Registry write failed for " + meta.getServiceName(), e);
        }
    }

    @Override
    public synchronized void deregister(ServiceMeta meta) {
        Path file = fileFor(meta.getServiceName());
        try {
            Map<String, Entry> entries = read(file);
            entries.remove(meta.endpoint());
            write(file, entries);
        } catch (IOException e) {
            throw new IllegalStateException("Registry deregistration failed for " + meta.getServiceName(), e);
        }
    }

    @Override
    public synchronized List<ServiceMeta> lookup(String serviceName) {
        List<ServiceMeta> alive = new ArrayList<>();
        long now = System.currentTimeMillis();
        try {
            for (Entry entry : read(fileFor(serviceName)).values()) {
                if (now - entry.lastSeen() <= ttlMillis) {
                    alive.add(ServiceMeta.parse(serviceName, entry.endpoint(), entry.lastSeen(), entry.weight()));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Registry read failed for " + serviceName, e);
        }
        return alive;
    }

    @Override
    public void close() {
    }
}
