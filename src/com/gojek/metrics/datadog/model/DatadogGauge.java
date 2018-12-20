package com.gojek.metrics.datadog.model;

import com.gojek.metrics.datadog.model.DatadogSeries;

import java.util.List;

public class DatadogGauge extends DatadogSeries<Number> {

  public DatadogGauge(String name, Number count, Long epoch, String host, List<String> additionalTags) {
    super(name, count, epoch, host, additionalTags);
  }

  public String getType() {
    return "gauge";
  }
}
