package org.blab.kafka.connect.tcp;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StringDelimitedConverter implements MessageConverter {
  @Override
  public SourceRecord convert(byte[] message) {
    var map =
        Arrays.stream(new String(message).split("\\|"))
            .map(s -> s.split(":"))
            .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    if (!map.containsKey("name")) throw new IllegalArgumentException();
    if (map.keySet().size() == 1) throw new IllegalArgumentException();

    var object = new JSONObject();
    var topic = convertTopic(map.get("name"));

    map.forEach((key, value) -> object.put(key, JSONObject.stringToValue(value)));

    return new SourceRecord(
        null, null, topic, Schema.STRING_SCHEMA, topic, Schema.STRING_SCHEMA, object.toString());
  }

  private String convertTopic(String topic) {
    return topic.replace("/", ".");
  }
}
