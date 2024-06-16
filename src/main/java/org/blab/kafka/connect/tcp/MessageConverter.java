package org.blab.kafka.connect.tcp;

import org.apache.kafka.connect.source.SourceRecord;

/** Converter used to serialize messages into {@link SourceRecord}s. */
public interface MessageConverter {
  SourceRecord convert(byte[] message) throws IllegalArgumentException;
}
