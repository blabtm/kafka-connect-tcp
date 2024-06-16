package org.blab.kafka.connect.tcp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageBuffer {
  private final ByteBuffer local;
  private final ByteBuffer extern;
  private final byte breaker;

  private boolean isTrash = false;

  public MessageBuffer(int maxMessageSize, byte breaker) {
    this.local = ByteBuffer.allocate(maxMessageSize + 1);
    this.extern = ByteBuffer.allocate(maxMessageSize + 1);
    this.breaker = breaker;
  }

  public ByteBuffer extern() {
    return extern;
  }

  public byte breaker() {
    return breaker;
  }

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

    return r;
  }
}
