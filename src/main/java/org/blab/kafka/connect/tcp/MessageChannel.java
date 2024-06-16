package org.blab.kafka.connect.tcp;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface MessageChannel extends Closeable {
  int MAX_MESSAGE_LENGTH_DEFAULT = 2048;

  CompletableFuture<Void> connect(InetSocketAddress remote);

  /**
   * Read a sequence of bytes from the channel.
   *
   * <p>The result does not contains braking bytes.
   *
   * @throws java.nio.channels.NotYetConnectedException if the connection is not established at the
   *     time of the call.
   * @throws java.nio.BufferOverflowException if the buffer limit is exceeded before the braking
   *     bytes occurs.
   */
  CompletableFuture<List<byte[]>> read();

  /**
   * Write a sequence of bytes into the channel.
   *
   * <p>Braking bytes are added to the end of the message.
   *
   * @throws java.nio.channels.NotYetConnectedException if the connection is not established at the
   *     time of the call.
   */
  CompletableFuture<Integer> write(byte[] message);
}
