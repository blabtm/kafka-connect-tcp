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
              "org.blab.kafka.connect.tcp.StringDelimiterConverter",
              ConfigDef.Importance.HIGH,
              "Specific Json converter class")
          .define(
              "message.size.max",
              ConfigDef.Type.INT,
              2048,
              ConfigDef.Importance.HIGH,
              "Message size limit")
          .define(
              "message.breaker",
              ConfigDef.Type.INT,
              (int) '\n',
              ConfigDef.Importance.HIGH,
              "Unsigned ASCII code for message breaker.")
          .define(
              "remote.topics",
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "List of topic to listen")
          .define(
              "remote.host",
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Remote service hostname")
          .define(
              "remote.port", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Remote service port")
          .define(
              "remote.reconnect.timeout.ms",
              ConfigDef.Type.LONG,
              30000,
              ConfigDef.Importance.HIGH,
              "Remote service reconnect attempt timeout")
          .define(
              "remote.cmd.subscribe",
              ConfigDef.Type.STRING,
              "name:%s|method:subscr\n",
              ConfigDef.Importance.HIGH,
              "Command pattern used to subscribe for a topic");

  public TcpSourceConfiguration(Map<?, ?> originals) {
    super(CONFIG_DEF, originals);
  }
}
