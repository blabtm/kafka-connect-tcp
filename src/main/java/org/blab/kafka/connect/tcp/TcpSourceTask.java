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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TcpSourceTask extends SourceTask {
  private static final Logger logger = LogManager.getLogger(TcpSourceTask.class);

  private final BlockingQueue<SourceRecord> queue = new LinkedBlockingQueue<>();
  private MessageChannel channel;
  private MessageConverter converter;

  @Override
  public void start(Map<String, String> map) {
    var configuration = new TcpSourceConfiguration(map);

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
          .whenCompleteAsync(
              (result, error) -> {
                if (error != null) logger.error(error);
                else {
                  configuration
                      .getList("remote.topics")
                      .forEach(
                          t ->
                              channel.write(
                                  String.format(configuration.getString("remote.cmd.subscribe"), t)
                                      .getBytes()));

                  channel.read().whenCompleteAsync(this::onMessage);
                }
              });

    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  private void onMessage(List<byte[]> result, Throwable error) {
    if (error != null) logger.error(error);
    else
      result.forEach(
          b -> {
            try {
              queue.add(converter.convert(b));
            } catch (IllegalArgumentException e) {
              logger.error(e);
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
      channel.close();
    } catch (IOException e) {
      logger.error(e);
    }
  }

  @Override
  public String version() {
    return TcpSourceConnector.VERSION;
  }
}
