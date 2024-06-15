package org.blab.kafka.connect.tcp;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public interface MessageChannel extends Closeable {
  int MAX_MESSAGE_LENGTH_DEFAULT = 2048;

  CompletableFuture<Void> connect(InetSocketAddress remote);

  /**
   * Read a sequence of bytes from the channel.
   *
   * <p>The result does not contains interrupting bytes.
   *
   * @throws java.nio.channels.NotYetConnectedException if the connection is not established at the
   *     time of the call.
   */
  CompletableFuture<byte[]> read();

  /**
   * Write a sequence of bytes into the channel.
   *
   * <p>Interrupting bytes are added to the end of message.
   *
   * @throws java.nio.channels.NotYetConnectedException if the connection is not established at the
   *     time of the call.
   */
  CompletableFuture<Integer> write(byte[] message);
}
