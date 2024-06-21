package org.blab.kafka.connect.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class VcasIntegrationTest {
  private static final MessageConverter converter = new StringDelimitedConverter();

  public static void main(String[] args) throws IOException, InterruptedException {
    MessageChannel channel =
        AsynchronousMessageChannel.open(new ByteBreakerMessageBuffer(2048, (byte) '\n'));

    channel
        .connect(new InetSocketAddress("172.16.1.110", 20041))
        .whenCompleteAsync(
            (res, exc) -> {
              if (exc != null) throw new RuntimeException(exc);
              channel.write("name:VEPP/QUAD/1F1/Volt|method:subscr\n".getBytes());
              channel.read().whenCompleteAsync((r, e) -> resultHandler(channel, r, e));
            });

    Thread.currentThread().join();
  }

  private static void resultHandler(MessageChannel channel, List<byte[]> result, Throwable t) {
    if (t != null) throw new RuntimeException(t);
    result.forEach(
        b -> {
          var r = converter.convert(b);
          System.out.printf("[%s]: %s\n", r.getKey(), new String(r.getValue()));
        });
    channel.read().whenCompleteAsync((r, e) -> resultHandler(channel, r, e));
  }
}
