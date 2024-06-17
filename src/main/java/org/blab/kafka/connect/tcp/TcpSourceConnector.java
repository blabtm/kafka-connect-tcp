package org.blab.kafka.connect.tcp;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TcpSourceConnector extends SourceConnector {
  public static final String VERSION = "0.0.1";
  private static final Logger logger = LogManager.getLogger(TcpSourceConnector.class);

  private TcpSourceConfiguration configuration;

  @Override
  public void start(Map<String, String> map) {
    logger.info("Staring...");
    configuration = new TcpSourceConfiguration(map);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int tasksMax) {
    logger.info("Configuring tasks...");
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
