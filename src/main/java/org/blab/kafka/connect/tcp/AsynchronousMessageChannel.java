package org.blab.kafka.connect.tcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AsynchronousMessageChannel implements MessageChannel {
  private static final Logger logger = LogManager.getLogger(AsynchronousMessageChannel.class);

  private final BlockingQueue<Task> tasks = new LinkedBlockingQueue<>();
  private final Lock readLock = new ReentrantLock();
  private final Lock writeLock = new ReentrantLock();

  private final AsynchronousSocketChannel socketChannel;
  private final MessageBuffer buffer;

  private AsynchronousMessageChannel(
      AsynchronousSocketChannel socketChannel, MessageBuffer buffer) {
    this.socketChannel = socketChannel;
    this.buffer = buffer;
  }

  public static AsynchronousMessageChannel open(MessageBuffer buffer) throws IOException {
    return new AsynchronousMessageChannel(AsynchronousSocketChannel.open(), buffer);
  }

  @Override
  public CompletableFuture<InetSocketAddress> connect(InetSocketAddress remote) {
    logger.info("Connecting to {}", remote);

    var future = new CompletableFuture<InetSocketAddress>();

    socketChannel.connect(
        remote,
        remote,
        new CompletionHandler<>() {
          @Override
          public void completed(Void unused, InetSocketAddress remote) {
            logger.info("Connected to {}", remote);
            future.complete(remote);
          }

          @Override
          public void failed(Throwable t, InetSocketAddress remote) {
            logger.error(t);
            future.completeExceptionally(t);
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<List<byte[]>> read() {
    var future = new CompletableFuture<List<byte[]>>();

    socketChannel.read(
        buffer.extern(),
        null,
        new CompletionHandler<Integer, Void>() {
          @Override
          public void completed(Integer size, Void a) {
            List<byte[]> r = buffer.commit();

            if (r.isEmpty()) socketChannel.read(buffer.extern(), null, this);
            else future.complete(r);
          }

          @Override
          public void failed(Throwable t, Void a) {
            logger.error(t);
            future.completeExceptionally(t);
          }
        });

    return future;
  }

  @Override
  public CompletableFuture<Integer> write(byte[] message) {
    logger.debug("Writing: {}", new String(message));

    var task = new Task(message, new CompletableFuture<>());

    tasks.add(task);
    if (writeLock.tryLock()) {
      socketChannel.write(
          ByteBuffer.wrap(task.message),
          task,
          new CompletionHandler<>() {
            @Override
            public void completed(Integer size, Task task) {
              logger.debug("Message written: {}", new String(task.message));
              task.future.complete(size);
              next();
            }

            @Override
            public void failed(Throwable t, Task task) {
              logger.error(t);
              task.future.completeExceptionally(t);
              next();
            }

            private void next() {
              var next = tasks.poll();
              if (next != null) socketChannel.write(ByteBuffer.wrap(next.message), next, this);
            }
          });
    }

    return task.future;
  }

  @Override
  public void close() {
    try {
      socketChannel.close();
    } catch (Exception e) {
      logger.warn(e);
    }

    tasks.forEach(
        t ->
            t.future.completeExceptionally(
                new CancellationException("Cancelled due interruption.")));
  }

  record Task(byte[] message, CompletableFuture<Integer> future) {}
}
