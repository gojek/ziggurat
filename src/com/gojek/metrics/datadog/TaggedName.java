package com.gojek.metrics.datadog;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TaggedName {
  private static final Pattern tagPattern = Pattern
      .compile("([\\w\\.]+)\\[([\\w\\W]+)\\]");

  private final String metricName;
  private final List<String> encodedTags;

  private TaggedName(String metricName, List<String> encodedTags) {
    this.metricName = metricName;
    this.encodedTags = encodedTags;
  }

  public String getMetricName() {
    return metricName;
  }

  public List<String> getEncodedTags() {
    return encodedTags;
  }

  public String encode() {
    if (!encodedTags.isEmpty()) {
      StringBuilder sb = new StringBuilder(this.metricName);
      sb.append('[');
      String prefix = "";
      for (String encodedTag : encodedTags) {
        sb.append(prefix);
        sb.append(encodedTag);
        prefix = ",";
      }
      sb.append(']');
      return sb.toString();
    } else {
      return this.metricName;
    }
  }

  public static com.gojek.metrics.datadog.TaggedName decode(String encodedTaggedName) {
    TaggedNameBuilder builder = new TaggedNameBuilder();

    Matcher matcher = tagPattern.matcher(encodedTaggedName);
    if (matcher.find() && matcher.groupCount() == 2) {
      builder.metricName(matcher.group(1));
      for(String t : matcher.group(2).split("\\,")) {
        builder.addTag(t);
      }
    } else {
      builder.metricName(encodedTaggedName);
    }

    return builder.build();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    com.gojek.metrics.datadog.TaggedName that = (com.gojek.metrics.datadog.TaggedName) o;

    if (metricName != null ? !metricName.equals(that.metricName) : that.metricName != null) return false;
    if (encodedTags != null ? !encodedTags.equals(that.encodedTags) : that.encodedTags != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = metricName != null ? metricName.hashCode() : 0;
    result = 31 * result + (encodedTags != null ? encodedTags.hashCode() : 0);
    return result;
  }


  public static class TaggedNameBuilder {
    private String metricName;
    private final List<String> encodedTags = new ArrayList<String>();

    public TaggedNameBuilder metricName(String metricName) {
      this.metricName = metricName;
      return this;
    }

    public TaggedNameBuilder addTag(String key, String val) {
      assertNonEmpty(key, "tagKey");
      encodedTags.add(new StringBuilder(key).append(':').append(val).toString());
      return this;
    }

    public TaggedNameBuilder addTag(String encodedTag) {
      assertNonEmpty(encodedTag, "encodedTag");
      encodedTags.add(encodedTag);
      return this;
    }

    private void assertNonEmpty(String s, String field) {
      if (s == null || "".equals(s.trim())) {
        throw new IllegalArgumentException((field + " must be defined"));
      }
    }

    public com.gojek.metrics.datadog.TaggedName build() {
      assertNonEmpty(this.metricName, "metricName");

      return new com.gojek.metrics.datadog.TaggedName(this.metricName, this.encodedTags);
    }
  }
}
