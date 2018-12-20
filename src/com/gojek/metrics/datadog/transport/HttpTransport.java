package com.gojek.metrics.datadog.transport;

import com.gojek.metrics.datadog.model.DatadogCounter;
import com.gojek.metrics.datadog.model.DatadogGauge;
import com.gojek.metrics.datadog.transport.Transport;
import com.gojek.metrics.serializer.JsonSerializer;
import com.gojek.metrics.serializer.Serializer;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.http.client.fluent.Request.Post;

/**
 * Uses the datadog http webservice to push metrics.
 *
 * @see <a href="http://docs.datadoghq.com/api/">API docs</a>
 */
public class HttpTransport implements Transport {

  private static final Logger LOG = LoggerFactory.getLogger(com.gojek.metrics.datadog.transport.HttpTransport.class);

  private final static String BASE_URL = "https://app.datadoghq.com/api/v1";
  private final String seriesUrl;
  private final int connectTimeout;     // in milliseconds
  private final int socketTimeout;      // in milliseconds
  private final HttpHost proxy;

  private HttpTransport(String apiKey, int connectTimeout, int socketTimeout, HttpHost proxy) {
    this.seriesUrl = String.format("%s/series?api_key=%s", BASE_URL, apiKey);
    this.connectTimeout = connectTimeout;
    this.socketTimeout = socketTimeout;
    this.proxy = proxy;
  }

  public static class Builder {
    String apiKey;
    int connectTimeout = 5000;
    int socketTimeout = 5000;
    HttpHost proxy;

    public Builder withApiKey(String key) {
      this.apiKey = key;
      return this;
    }

    public Builder withConnectTimeout(int milliseconds) {
      this.connectTimeout = milliseconds;
      return this;
    }

    public Builder withSocketTimeout(int milliseconds) {
      this.socketTimeout = milliseconds;
      return this;
    }

    public Builder withProxy(String proxyHost, int proxyPort) {
      this.proxy = new HttpHost(proxyHost, proxyPort);
      return this;
    }

    public com.gojek.metrics.datadog.transport.HttpTransport build() {
      return new com.gojek.metrics.datadog.transport.HttpTransport(apiKey, connectTimeout, socketTimeout, proxy);
    }
  }

  public Request prepare() throws IOException {
    return new HttpRequest(this);
  }

  public void close() throws IOException {
  }

  public static class HttpRequest implements Transport.Request {
    protected final Serializer serializer;

    protected final com.gojek.metrics.datadog.transport.HttpTransport transport;

    public HttpRequest(com.gojek.metrics.datadog.transport.HttpTransport transport) throws IOException {
      this.transport = transport;
      serializer = new JsonSerializer();
      serializer.startObject();
    }

    public void addGauge(DatadogGauge gauge) throws IOException {
      serializer.appendGauge(gauge);
    }

    public void addCounter(DatadogCounter counter) throws IOException {
      serializer.appendCounter(counter);
    }

    public void send() throws Exception {
      serializer.endObject();
      String postBody = serializer.getAsString();
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append("Sending HTTP POST request to ");
        sb.append(this.transport.seriesUrl);
        sb.append(", POST body is: \n");
        sb.append(postBody);
        LOG.debug(sb.toString());
      }
      long start = System.currentTimeMillis();
      org.apache.http.client.fluent.Request request = Post(this.transport.seriesUrl)
        .useExpectContinue()
        .connectTimeout(this.transport.connectTimeout)
        .socketTimeout(this.transport.socketTimeout)
        .bodyString(postBody, ContentType.APPLICATION_JSON);

      if (this.transport.proxy != null) {
        request.viaProxy(this.transport.proxy);
      }

      Response response = request.execute();

      long elapsed = System.currentTimeMillis() - start;

      if (LOG.isDebugEnabled()) {
        HttpResponse httpResponse = response.returnResponse();
        StringBuilder sb = new StringBuilder();

        sb.append("Sent metrics to Datadog: ");
        sb.append("  Timing: ").append(elapsed).append(" ms\n");
        sb.append("  Status: ").append(httpResponse.getStatusLine().getStatusCode()).append("\n");

        String content = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        sb.append("  Content: ").append(content);

        LOG.debug(sb.toString());
      } else {
        response.discardContent();
      }
    }
  }
}
