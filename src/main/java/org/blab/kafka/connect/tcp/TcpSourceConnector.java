package org.blab.kafka.connect.tcp;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TcpSourceConnector extends SourceConnector {
  public static final String VERSION = "0.0.1";

  private TcpSourceConfiguration configuration;

  @Override
  public void start(Map<String, String> map) {
    configuration = new TcpSourceConfiguration(map);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int tasksMax) {
    return ConnectorUtils.groupPartitions(configuration.getList("remote.topics"), tasksMax).stream()
        .map(
            g -> {
              Map<String, String> map = new HashMap<>(configuration.originalsStrings());
              map.put("remote.topics", String.join(",", g));
              return map;
            })
        .toList();
  }

  @Override
  public ConfigDef config() {
    return TcpSourceConfiguration.CONFIG_DEF;
  }

  @Override
  public void stop() {}

  @Override
  public Class<? extends Task> taskClass() {
    return TcpSourceTask.class;
  }

  @Override
  public String version() {
    return VERSION;
  }
}
