package org.blab.kafka.connect.tcp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** {@link MessageBuffer} for byte separated message streams. */
public class ByteBreakerMessageBuffer extends MessageBuffer {
  private final ByteBuffer local;
  private final byte breaker;

  private boolean isTrash = false;

  public ByteBreakerMessageBuffer(int maxMessageSize, byte breaker) {
    super(ByteBuffer.allocate(maxMessageSize + 1));
    this.local = ByteBuffer.allocate(maxMessageSize + 1);
    this.breaker = breaker;
  }

  @Override
  public List<byte[]> commit() {
    var r = new ArrayList<byte[]>();

    for (int i = 0; i < extern.position(); ++i) {
      byte b = extern.get(i);

      if (b == breaker) {
        if (isTrash) isTrash = false;
        else {
          r.add(Arrays.copyOf(local.array(), local.position()));
          local.clear();
        }
      } else if (!isTrash) {
        local.put(b);

        if (!local.hasRemaining()) {
          isTrash = true;
          local.clear();
        }
      }
    }

    extern.clear();
    return r;
  }
}
