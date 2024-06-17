package org.blab.kafka.connect.tcp;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TcpSourceTask extends SourceTask {
  private static final Logger logger = LogManager.getLogger(TcpSourceTask.class);

  private final BlockingQueue<SourceRecord> queue = new LinkedBlockingQueue<>();
  private MessageChannel channel;
  private MessageConverter converter;
  private TcpSourceConfiguration configuration;

  @Override
  public void start(Map<String, String> map) {
    logger.info("Starting...");

    configuration = new TcpSourceConfiguration(map);

    try {
      converter =
          Class.forName(configuration.getString("converter.class"))
              .asSubclass(MessageConverter.class)
              .getConstructor()
              .newInstance();

      channel =
          AsynchronousMessageChannel.open(
              new ByteBreakerMessageBuffer(
                  configuration.getInt("message.size.max"),
                  configuration.getInt("message.breaker").byteValue()));

      channel
          .connect(
              new InetSocketAddress(
                  configuration.getString("remote.host"), configuration.getInt("remote.port")))
          .whenCompleteAsync(this::onConnected);

    } catch (Exception e) {
      logger.fatal(e);
      throw new ConnectException(e);
    }
  }

  private void onConnected(InetSocketAddress result, Throwable error) {
    if (error != null) {
      logger.error("Failed to connect to remote server: {}", result, error);

      new Timer(true)
          .schedule(
              new TimerTask() {
                @Override
                public void run() {
                  logger.info("Trying to reconnect...");
                  channel.connect(result).whenCompleteAsync(TcpSourceTask.this::onConnected);
                }
              },
              configuration.getLong("remote.reconnect.timeout.ms"));
    } else {
      logger.info("Successfully connected to remote server: {}", result);
      configuration
          .getList("remote.topics")
          .forEach(
              t ->
                  channel
                      .write(
                          String.format(configuration.getString("remote.cmd.subscribe"), t)
                              .getBytes())
                      .whenCompleteAsync(
                          (r, e) -> {
                            if (e != null) logger.error("Error writing to remote server.", e);
                            else logger.info("Message written to remote server.");
                          }));

      channel.read().whenCompleteAsync(this::onMessage);
    }
  }

  private void onMessage(List<byte[]> result, Throwable error) {
    if (error != null) logger.error("Error reading from remote server.", error);
    else
      result.forEach(
          b -> {
            try {
              queue.add(converter.convert(b));
            } catch (IllegalArgumentException e) {
              logger.error("Failed converting message.", e);
            }
          });

    channel.read().whenCompleteAsync(this::onMessage);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    return List.of(queue.take());
  }

  @Override
  public void stop() {
    try {
      if (channel != null) channel.close();
    } catch (IOException e) {
      logger.error(e);
    }
  }

  @Override
  public String version() {
    return TcpSourceConnector.VERSION;
  }
}
