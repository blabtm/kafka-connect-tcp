package org.blab.kafka.connect.tcp;

import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class StringDelimitedConverter implements MessageConverter {
  public static final String TIME_FMT = "DD.MM.YYYY HH_mm_ss.SSS";

  @Override
  public Map.Entry<String, byte[]> convert(byte[] message) {
    var map =
        Arrays.stream(new String(message).split("\\|"))
            .map(s -> s.split(":"))
            .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    if (!map.containsKey("name"))
      throw new IllegalArgumentException(
          "Required field missed: \"name\", message: " + new String(message));

    return Map.entry(convertTopic(map.get("name")), toJson(map).toString().getBytes());
  }

  private JSONObject toJson(Map<String, String> pure) {
    var object = new JSONObject();

    pure.forEach(
        (key, value) -> {
          switch (key) {
            case "name":
              break;
            case "time":
              object.put("timestamp", convertTimestamp(value));
              break;
            case "val":
              object.put("value", JSONObject.stringToValue(value));
              break;
            default:
              object.put(key, JSONObject.stringToValue(value));
              break;
          }
        });

    return object;
  }

  private String convertTopic(String topic) {
    return topic.replace("/", ".").toLowerCase().trim();
  }

  private Long convertTimestamp(String time) {
    try {
      return new SimpleDateFormat(TIME_FMT).parse(time).getTime();
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
