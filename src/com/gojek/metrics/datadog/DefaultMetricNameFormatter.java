package com.gojek.metrics.datadog;

import com.gojek.metrics.datadog.MetricNameFormatter;

public class DefaultMetricNameFormatter implements MetricNameFormatter {

  public String format(String name, String... path) {
    final StringBuilder sb = new StringBuilder();

    String[] metricParts = name.split("\\[");
    sb.append(metricParts[0]);

    for (String part : path) {
        sb.append('.').append(part);
    }

    for (int i = 1; i < metricParts.length; i++) {
        sb.append('[').append(metricParts[i]);
    }
    return sb.toString();
  }
}