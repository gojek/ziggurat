package com.gojek.metrics.serializer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.metrics.datadog.model.DatadogCounter;
import com.gojek.metrics.datadog.model.DatadogGauge;
import com.gojek.metrics.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Serialize datadog time series object into json
 *
 * @see <a href="http://docs.datadoghq.com/api/">API docs</a>
 */
public class JsonSerializer implements Serializer {
  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(JSON_FACTORY);
  private static final Logger LOG = LoggerFactory.getLogger(com.gojek.metrics.serializer.JsonSerializer.class);

  private JsonGenerator jsonOut;
  private ByteArrayOutputStream outputStream;

  public void startObject() throws IOException {
    outputStream = new ByteArrayOutputStream(2048);
    jsonOut = JSON_FACTORY.createGenerator(outputStream);
    jsonOut.writeStartObject();
    jsonOut.writeArrayFieldStart("series");
  }

  public void appendGauge(DatadogGauge gauge) throws IOException {
    MAPPER.writeValue(jsonOut, gauge);
  }

  public void appendCounter(DatadogCounter counter) throws IOException {
    MAPPER.writeValue(jsonOut, counter);
  }

  public void endObject() throws IOException {
    jsonOut.writeEndArray();
    jsonOut.writeEndObject();
    jsonOut.flush();
    outputStream.close();
  }

  public String getAsString() throws UnsupportedEncodingException {
    return outputStream.toString("UTF-8");
  }
}
