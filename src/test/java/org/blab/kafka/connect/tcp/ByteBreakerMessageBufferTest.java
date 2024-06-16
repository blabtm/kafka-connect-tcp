package org.blab.kafka.connect.tcp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ByteBreakerMessageBufferTest {
  private MessageBuffer buffer;

  @BeforeEach
  public void beforeEach() {
    buffer = new ByteBreakerMessageBuffer(5, (byte) '\n');
  }

  @Test
  public void commitWithMessage() {
    buffer.extern().put(new byte[] {'h', 'e', 'l', 'l', 'o', '\n'});
    var messages = buffer.commit();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertArrayEquals(new byte[] {'h', 'e', 'l', 'l', 'o'}, messages.getFirst());
  }

  @Test
  public void commitWithOverflow() {
    buffer.extern().put(new byte[] {'h', 'e', 'l', 'l', 'o'});
    Assertions.assertEquals(0, buffer.commit().size());

    buffer.extern().put(new byte[] {'h', 'e', 'l', 'l', 'o'});
    Assertions.assertEquals(0, buffer.commit().size());

    buffer.extern().put(new byte[] {'h', 'e', '\n', 'h', 'i', '\n'});
    var messages = buffer.commit();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertArrayEquals(new byte[] {'h', 'i'}, messages.getFirst());
  }

  @Test
  public void commitWithPartialMessage() {
    buffer.extern().put(new byte[] {'h', 'e', 'l'});
    Assertions.assertEquals(0, buffer.commit().size());

    buffer.extern().put(new byte[] {'l', 'o', '\n', 'l', 'o'});
    var messages = buffer.commit();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertArrayEquals(new byte[] {'h', 'e', 'l', 'l', 'o'}, messages.getFirst());
  }

  @Test
  public void commitWithEmptyMessage() {
    buffer.extern().put(new byte[] {'h', 'i', '\n', '\n', '\n'});
    var message = buffer.commit();
    Assertions.assertEquals(3, message.size());
    Assertions.assertArrayEquals(new byte[] {'h', 'i'}, message.getFirst());
    Assertions.assertArrayEquals(new byte[0], message.get(1));
    Assertions.assertArrayEquals(new byte[0], message.get(2));
  }
}
