package dev.rainsolo.minirpc.protocol;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Compact header codec: {@code k=v;k=v}. Values are sanitized so a malformed header
 * can never desynchronize the length-prefixed frame parser.
 */
public final class Headers {

    private Headers() {
    }

    public static byte[] encode(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return new byte[0];
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            builder.append(sanitize(entry.getKey()))
                    .append('=')
                    .append(sanitize(entry.getValue()))
                    .append(';');
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static Map<String, String> decode(byte[] data) {
        Map<String, String> headers = new HashMap<>();
        if (data == null || data.length == 0) {
            return headers;
        }
        String text = new String(data, StandardCharsets.UTF_8);
        for (String part : text.split(";")) {
            int separator = part.indexOf('=');
            if (separator > 0) {
                headers.put(part.substring(0, separator), part.substring(separator + 1));
            }
        }
        return headers;
    }

    private static String sanitize(String value) {
        return value.replace(";", "_").replace("=", "_");
    }
}
