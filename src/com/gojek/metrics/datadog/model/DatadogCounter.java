package com.gojek.metrics.datadog.model;

import com.gojek.metrics.datadog.model.DatadogSeries;

import java.util.List;

public class DatadogCounter extends DatadogSeries<Long> {
  
  public DatadogCounter(String name, Long count, Long epoch, String host, List<String> additionalTags) {
    super(name, count, epoch, host, additionalTags);
  }

  public String getType() {
    return "counter";
  }
}