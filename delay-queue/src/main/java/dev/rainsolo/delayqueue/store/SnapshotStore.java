package dev.rainsolo.delayqueue.store;

import dev.rainsolo.delayqueue.core.Clock;
import dev.rainsolo.delayqueue.core.DelayedMessage;
import dev.rainsolo.delayqueue.core.MessageStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Persists unfinished messages as relative remaining delays, not absolute timestamps.
 * On restore the deadline is recomputed against the current monotonic clock, which is
 * why a snapshot taken before a wall-clock change or a restart stays correct.
 */
public final class SnapshotStore {

    public record Entry(String id, String topic, String payload, long remainingMillis,
                        int attempts, int maxAttempts, long createdWallMillis) {
    }

    private final Path file;

    public SnapshotStore(Path file) {
        this.file = file;
    }

    public void write(List<DelayedMessage> messages, Clock clock) throws IOException {
        StringBuilder builder = new StringBuilder();
        long now = clock.nanoTime();
        for (DelayedMessage message : messages) {
            if (message.getStatus() != MessageStatus.PENDING
                    && message.getStatus() != MessageStatus.DELIVERING) {
                continue;
            }
            long remaining = Math.max(0, (message.getDeadlineNanos() - now) / 1_000_000L);
            builder.append(encode(message.getId())).append('|')
                    .append(encode(message.getTopic())).append('|')
                    .append(encode(message.getPayload())).append('|')
                    .append(remaining).append('|')
                    .append(message.getAttempts()).append('|')
                    .append(message.getMaxAttempts()).append('|')
                    .append(message.getCreatedWallMillis()).append('\n');
        }
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(temp, builder.toString(), StandardCharsets.UTF_8);
        try {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public List<Entry> read() throws IOException {
        List<Entry> entries = new ArrayList<>();
        if (!Files.exists(file)) {
            return entries;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.split("\\|", -1);
            if (parts.length != 7) {
                continue;
            }
            try {
                entries.add(new Entry(decode(parts[0]), decode(parts[1]), decode(parts[2]),
                        Long.parseLong(parts[3]), Integer.parseInt(parts[4]),
                        Integer.parseInt(parts[5]), Long.parseLong(parts[6])));
            } catch (RuntimeException ignored) {
                // Skip a corrupted line instead of losing the whole snapshot.
            }
        }
        return entries;
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
