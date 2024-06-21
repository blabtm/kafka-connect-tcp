package org.blab.kafka.connect.tcp;

import java.util.Map;

/**
 * Converter used to serialize messages into {@link Map.Entry}s consists of topic and parsed
 * message.
 */
public interface MessageConverter {
  Map.Entry<String, byte[]> convert(byte[] message) throws IllegalArgumentException;
}
