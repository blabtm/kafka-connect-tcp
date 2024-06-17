package org.blab.kafka.connect.tcp;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class TcpSourceConfiguration extends AbstractConfig {
  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              "converter.class",
              ConfigDef.Type.STRING,
              "org.blab.kafka.connect.tcp.StringDelimitedConverter",
              ConfigDef.Importance.HIGH,
              "Message specific JSON converter class.")
          .define(
              "message.size.max.bytes",
              ConfigDef.Type.INT,
              2048,
              ConfigDef.Importance.MEDIUM,
              "Message size limit in bytes.")
          .define(
              "message.breaker",
              ConfigDef.Type.INT,
              10,
              ConfigDef.Importance.HIGH,
              "Unsigned ASCII message breaker code.")
          .define(
              "remote.topics",
              ConfigDef.Type.LIST,
              ConfigDef.Importance.HIGH,
              "List of targeted topics.")
          .define(
              "remote.host",
              ConfigDef.Type.STRING,
              "localhost",
              ConfigDef.Importance.HIGH,
              "Target service hostname.")
          .define(
              "remote.port",
              ConfigDef.Type.INT,
              8080,
              ConfigDef.Importance.HIGH,
              "Target service port.")
          .define(
              "remote.reconnect.timeout.ms",
              ConfigDef.Type.LONG,
              30000L,
              ConfigDef.Importance.LOW,
              "Failover reconnect timeout.")
          .define(
              "remote.cmd.subscribe",
              ConfigDef.Type.STRING,
              "name:%s|method:subscr\n",
              ConfigDef.Importance.HIGH,
              "Subscription command pattern.");

  public TcpSourceConfiguration(Map<?, ?> originals) {
    super(CONFIG_DEF, originals);
  }
}
