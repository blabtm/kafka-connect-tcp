package org.blab.kafka.connect.tcp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class AsynchronousMessageChannelTest {
  private static AsynchronousMessageChannel channel;
  private static AsynchronousSocketChannel socketChannel;

  @BeforeAll
  public static void beforeAll() throws IOException, NoSuchFieldException, IllegalAccessException {
    socketChannel = mock(AsynchronousSocketChannel.class);
    channel = AsynchronousMessageChannel.open(new ByteBreakerMessageBuffer(10, (byte) '\n'));

    setSocketChannel(channel, socketChannel);
  }

  private static void setSocketChannel(AsynchronousMessageChannel mc, AsynchronousSocketChannel sc)
      throws NoSuchFieldException, IllegalAccessException {
    var socketChannelField = mc.getClass().getDeclaredField("socketChannel");
    socketChannelField.setAccessible(true);
    socketChannelField.set(mc, sc);
  }

  @Test
  public void connectOk() {
    doAnswer(
            call -> {
              call.getArgument(2, CompletionHandler.class).completed(null, call.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .connect(any(SocketAddress.class), any(), any(CompletionHandler.class));

    Assertions.assertDoesNotThrow(
        () -> channel.connect(new InetSocketAddress("localhost", 8080)).get());
  }

  @Test
  public void connectIOException() {
    doAnswer(
            call -> {
              call.getArgument(2, CompletionHandler.class)
                  .failed(new IOException(), call.getArgument(1));
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
  public void readInSingleIterationOk() {
    doAnswer(
            call -> {
              call.getArgument(0, ByteBuffer.class).put(new byte[] {'h', 'i', '\n'});
              call.getArgument(2, CompletionHandler.class).completed(3, call.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    List<byte[]> messages = Assertions.assertDoesNotThrow(() -> channel.read().get());
    Assertions.assertArrayEquals(new byte[] {'h', 'i'}, messages.getFirst());
  }

  @Test
  public void readInSeveralIterationsOk() {
    AtomicInteger attempt = new AtomicInteger(0);
    byte[] message = new byte[] {'h', 'e', 'l', 'l', 'o', '\n'};

    doAnswer(
            call -> {
              if (attempt.get() < message.length) {
                call.getArgument(0, ByteBuffer.class).put(message[attempt.getAndIncrement()]);
                call.getArgument(2, CompletionHandler.class).completed(1, call.getArgument(1));
              }

              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    List<byte[]> messages = Assertions.assertDoesNotThrow(() -> channel.read().get());
    Assertions.assertArrayEquals(new byte[] {'h', 'e', 'l', 'l', 'o'}, messages.getFirst());
  }

  @Test
  public void readIOException() {
    doAnswer(
            call -> {
              call.getArgument(2, CompletionHandler.class)
                  .failed(new IOException(), call.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .read(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    ExecutionException e =
        Assertions.assertThrows(ExecutionException.class, () -> channel.read().get());
    Assertions.assertInstanceOf(IOException.class, e.getCause());
  }

  @Test
  public void writeOk() {
    doAnswer(
            call -> {
              int size = call.getArgument(0, ByteBuffer.class).capacity();
              call.getArgument(2, CompletionHandler.class).completed(size, call.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .write(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    int result =
        Assertions.assertDoesNotThrow(() -> channel.write(new byte[] {'h', 'i', '\n'}).get());
    Assertions.assertEquals(3, result);
  }

  @Test
  public void writeIOException() {
    doAnswer(
            call -> {
              call.getArgument(2, CompletionHandler.class)
                  .failed(new IOException(), call.getArgument(1));
              return null;
            })
        .when(socketChannel)
        .write(any(ByteBuffer.class), any(), any(CompletionHandler.class));

    ExecutionException e =
        Assertions.assertThrows(ExecutionException.class, () -> channel.write(new byte[] {}).get());
    Assertions.assertInstanceOf(IOException.class, e.getCause());
  }
}
