package org.blab.kafka.connect.tcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetConnectedException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class AsynchronousMessageChannel implements MessageChannel {
  private static final Logger logger = LogManager.getLogger(AsynchronousMessageChannel.class);

  private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
  private final ReentrantLock readLock = new ReentrantLock();
  private final AtomicBoolean isConnected = new AtomicBoolean(false);

  private final AsynchronousSocketChannel socketChannel;
  private final MessageBuffer buffer;

  private AsynchronousMessageChannel(
      AsynchronousSocketChannel socketChannel, byte breaker, int maxMessageSize) {
    this.socketChannel = socketChannel;
    this.buffer = new MessageBuffer(maxMessageSize, breaker);
  }

  public static AsynchronousMessageChannel open(byte breaker) throws IOException {
    return open(breaker, MessageChannel.MAX_MESSAGE_LENGTH_DEFAULT);
  }

  public static AsynchronousMessageChannel open(byte breaker, int maxMessageSize)
      throws IOException {
    return new AsynchronousMessageChannel(
        AsynchronousSocketChannel.open(), breaker, maxMessageSize);
  }

  @Override
  public CompletableFuture<Void> connect(InetSocketAddress remote) {
    var future = new CompletableFuture<Void>();

    socketChannel.connect(
        remote,
        null,
        new CompletionHandler<Void, Void>() {
          @Override
          public void completed(Void unused, Void unused2) {
            isConnected.set(true);
            future.complete(null);

            socketChannel.write(
                ByteBuffer.wrap(new byte[0]),
                null,
                new CompletionHandler<Integer, Task>() {
                  @Override
                  public void completed(Integer size, Task task) {
                    if (task != null) task.future.complete(size);
                    next();
                  }

                  @Override
                  public void failed(Throwable t, Task task) {
                    if (task != null) task.future.completeExceptionally(t);
                    next();
                  }

                  private void next() {
                    try {
                      var next = taskQueue.take();
                      var msg = Arrays.copyOf(next.message, next.message.length + 1);
                      msg[next.message.length] = buffer.breaker();
                      socketChannel.write(ByteBuffer.wrap(msg), next, this);
                    } catch (InterruptedException e) {
                      logger.info("Interrupted while waiting for next task", e);
                    }
                  }
                });
          }

          @Override
          public void failed(Throwable t, Void unused) {
            isConnected.set(false);
            future.completeExceptionally(t);
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<List<byte[]>> read() {
    if (!isConnected.get()) return CompletableFuture.failedFuture(new NotYetConnectedException());
    readLock.lock();

    var future = new CompletableFuture<List<byte[]>>();

    socketChannel.read(
        buffer.extern(),
        null,
        new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer size, Void a) {
            List<byte[]> r = buffer.commit();

            if (r.isEmpty()) socketChannel.read(buffer.extern(), null, this);
            else {
              future.complete(r);
              readLock.unlock();
            }
          }

          @Override
          public void failed(Throwable t, Void a) {
            future.completeExceptionally(t);
            readLock.unlock();
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<Integer> write(byte[] message) {
    if (!isConnected.get()) return CompletableFuture.failedFuture(new NotYetConnectedException());
    var task = new Task(message, new CompletableFuture<>());
    taskQueue.offer(task);
    return task.future;
  }

  @Override
  public void close() throws IOException {
    socketChannel.close();
  }

  record Task(byte[] message, CompletableFuture<Integer> future) {}
}
