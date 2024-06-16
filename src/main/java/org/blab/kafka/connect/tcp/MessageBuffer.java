package org.blab.kafka.connect.tcp;

import java.nio.ByteBuffer;
import java.util.List;

/** {@link ByteBuffer} extension to extract byte sequences in specific way. */
public abstract class MessageBuffer {
  protected final ByteBuffer extern;

  protected MessageBuffer(ByteBuffer extern) {
    this.extern = extern;
  }

  /** Get source {@link ByteBuffer} for direct writing. */
  public ByteBuffer extern() {
    return this.extern;
  }

  /**
   * Fetch changes in source {@link ByteBuffer} since last commit and return newly extracted
   * messages.
   */
  public abstract List<byte[]> commit();
}
