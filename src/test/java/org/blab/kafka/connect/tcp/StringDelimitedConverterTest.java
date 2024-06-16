package org.blab.kafka.connect.tcp;

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringDelimitedConverterTest {
  private static final MessageConverter converter = new StringDelimitedConverter();

  @Test
  public void convertTopicMissedTest() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.convert("value:hello".getBytes()));
  }

  @Test
  public void convertZeroFields() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.convert("name:topic".getBytes()));
  }

  @Test
  public void convertNumericTest() {
    var record =
        Assertions.assertDoesNotThrow(() -> converter.convert("name:events|value:123".getBytes()));

    Assertions.assertEquals("events", record.topic());

    JSONObject json = new JSONObject((String) record.value());

    Assertions.assertEquals(json.getDouble("value"), 123.0);
  }
}
