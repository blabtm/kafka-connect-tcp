package org.blab.kafka.connect.tcp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetConnectedException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class AsynchronousMessageChannelTest {
  private static AsynchronousMessageChannel channel;
  private static AsynchronousSocketChannel socketChannel;

  @BeforeAll
  public static void beforeAll() throws IOException, NoSuchFieldException, IllegalAccessException {
    socketChannel = mockSocketChannel();
    channel = createMessageChannel(socketChannel);
  }

  private static AsynchronousMessageChannel createMessageChannel(
      AsynchronousSocketChannel socketChannel)
      throws IOException, IllegalAccessException, NoSuchFieldException {
    var messageChannel = AsynchronousMessageChannel.open((byte) '\n', 5);
    var socketChannelField = messageChannel.getClass().getDeclaredField("socketChannel");

    socketChannelField.setAccessible(true);
    socketChannelField.set(messageChannel, socketChannel);

    return messageChannel;
  }

  private static AsynchronousSocketChannel mockSocketChannel() {
    return mock(AsynchronousSocketChannel.class);
  }

  private static void setConnected(Boolean connected) {
    try {
      var isConnectedField = channel.getClass().getDeclaredField("isConnected");

      isConnectedField.setAccessible(true);
      ((AtomicBoolean) isConnectedField.get(channel)).set(connected);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void connectOk() {
    doAnswer(
            q -> {
              q.getArgument(2, CompletionHandler.class).completed(null, q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .connect(any(SocketAddress.class), any(), any(CompletionHandler.class));

    Assertions.assertDoesNotThrow(
        () -> channel.connect(new InetSocketAddress("localhost", 8080)).get());
  }

  @Test
  public void connectNoRouteToHostException() {
    doAnswer(
            q -> {
              q.getArgument(2, CompletionHandler.class)
                  .failed(new NoRouteToHostException(), q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .connect(any(SocketAddress.class), any(), any(CompletionHandler.class));

    ExecutionException e =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> channel.connect(new InetSocketAddress("localhost", 8080)).get());
    Assertions.assertInstanceOf(NoRouteToHostException.class, e.getCause());
  }

  @Test
  public void connectIOException() {
    setConnected(false);

    doAnswer(
            q -> {
              q.getArgument(2, CompletionHandler.class).failed(new IOException(), q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .connect(any(SocketAddress.class), any(), any(CompletionHandler.class));

    ExecutionException e =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> channel.connect(new InetSocketAddress("localhost", 8080)).get());
    Assertions.assertInstanceOf(IOException.class, e.getCause());
  }

  @Test
  public void connectAlreadyConnectedException() {
    setConnected(true);

    ExecutionException e =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> channel.connect(new InetSocketAddress("localhost", 8080)).get());
    Assertions.assertInstanceOf(AlreadyConnectedException.class, e.getCause());
  }

  @Test
  public void readFromFirstAttemptOk() {
    setConnected(true);

    doAnswer(
            q -> {
              q.getArgument(0, ByteBuffer.class).put(new byte[] {'h', 'i', '\n'});
              q.getArgument(2, CompletionHandler.class).completed(3, q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    List<byte[]> messages = Assertions.assertDoesNotThrow(() -> channel.read().get());
    Assertions.assertArrayEquals(new byte[] {'h', 'i'}, messages.getFirst());
  }

  @Test
  public void readFromSecondAttemptOk() {
    setConnected(true);

    AtomicInteger attempt = new AtomicInteger(0);

    doAnswer(
            q -> {
              if (attempt.getAndIncrement() == 0) {
                q.getArgument(0, ByteBuffer.class).put(new byte[] {'h'});
                q.getArgument(2, CompletionHandler.class).completed(1, q.getArgument(1));
              } else {
                q.getArgument(0, ByteBuffer.class).put(new byte[] {'i', '\n'});
                q.getArgument(2, CompletionHandler.class).completed(2, q.getArgument(1));
              }

              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    List<byte[]> messages = Assertions.assertDoesNotThrow(() -> channel.read().get());
    Assertions.assertArrayEquals(new byte[] {'h', 'i'}, messages.getFirst());
  }

  @Test
  public void readFromMultipleAttemptsOk() {
    setConnected(true);

    var attempt = new AtomicInteger(0);
    var msg = new byte[] {'h', 'e', 'l', 'l', 'o', '\n'};

    doAnswer(
            q -> {
              if (attempt.getAndIncrement() <= msg.length) {
                q.getArgument(0, ByteBuffer.class).put(msg[attempt.get()]);
                q.getArgument(2, CompletionHandler.class).completed(1, q.getArgument(1));
              } else q.getArgument(2, CompletionHandler.class).completed(0, q.getArgument(1));

              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    List<byte[]> messages = Assertions.assertDoesNotThrow(() -> channel.read().get());
    Assertions.assertArrayEquals(new byte[] {'h', 'e', 'l', 'l', 'o'}, messages.getFirst());
  }

  @Test
  public void readAfterBufferOverflowCleanOk() {
    setConnected(true);

    var attempt = new AtomicInteger(0);

    doAnswer(
            q -> {
              if (attempt.getAndIncrement() == 0) {
                q.getArgument(0, ByteBuffer.class).put(new byte[] {'h', 'e', 'l', 'l', 'l', 'o'});
                q.getArgument(2, CompletionHandler.class).completed(6, q.getArgument(1));
              } else {
                q.getArgument(0, ByteBuffer.class).put(new byte[] {'h', '\n', 'h', 'i', '\n'});
                q.getArgument(2, CompletionHandler.class).completed(5, q.getArgument(1));
              }

              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    Assertions.assertThrows(BufferOverflowException.class, () -> channel.read().get());
    List<byte[]> messages = Assertions.assertDoesNotThrow(() -> channel.read().get());
    Assertions.assertArrayEquals(new byte[] {'h', 'i'}, messages.getFirst());
  }

  @Test
  public void readIOException() {
    setConnected(true);

    doAnswer(
            q -> {
              q.getArgument(2, CompletionHandler.class).failed(new IOException(), q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    ExecutionException e =
        Assertions.assertThrows(ExecutionException.class, () -> channel.read().get());
    Assertions.assertInstanceOf(IOException.class, e.getCause());
  }

  @Test
  public void readNotYetConnectedException() {
    setConnected(false);

    ExecutionException e =
        Assertions.assertThrows(ExecutionException.class, () -> channel.read().get());
    Assertions.assertInstanceOf(NotYetConnectedException.class, e.getCause());
  }

  @Test
  public void writeOk() {
    setConnected(true);

    doAnswer(
            q -> {
              int size = q.getArgument(0, ByteBuffer.class).position();
              q.getArgument(2, CompletionHandler.class).completed(size + 1, q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .write(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    int result = Assertions.assertDoesNotThrow(() -> channel.write(new byte[] {'h', 'i'}).get());
    Assertions.assertEquals(3, result);
  }

  @Test
  public void writeNotYetConnectedException() {
    setConnected(false);

    ExecutionException e =
        Assertions.assertThrows(ExecutionException.class, () -> channel.write(new byte[] {}));
    Assertions.assertInstanceOf(NotYetConnectedException.class, e.getCause());
  }

  @Test
  public void writeIOException() {
    setConnected(true);

    doAnswer(
            q -> {
              q.getArgument(2, CompletionHandler.class).failed(new IOException(), q.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .write(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    ExecutionException e =
        Assertions.assertThrows(ExecutionException.class, () -> channel.write(new byte[] {}));
    Assertions.assertInstanceOf(IOException.class, e.getCause());
  }
}
