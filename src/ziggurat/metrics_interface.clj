(ns ziggurat.metrics-interface)

(defprotocol MetricsProtocol
  "A protocol that defines the interface for statsd metrics implementation libraries. Any type or class that implements this protocol can be passed to the metrics namespace to be used for reporting metrics to statsd.
  Example implementations for this protocol: ziggurat.clj-statsd-metrics-wrapper/CljStatsd, ziggurat.dropwizard-metrics-wrapper/DropwizardMetrics

  initialize [impl statsd-config]
  This is used to initialize the metrics library so that it can push metrics to the telegraf instance.
  Args:
    - impl : the class object that implements this protocol, i.e. an instance of the deftype for example
    - statsd-config : the configuration required to initialize the metrics library. It is a map {:host \"localhost\" :port 8125 :enabled true}. The config map is read from the config {:ziggurat {:statsd {}}} path.

  terminate [impl]
  This is the function to ensure clean shutdown of the library and thus the application. It is called when the service is being stopped.
  - impl : the class object that implements this protocol, i.e. an instance of the deftype for example

  update-counter [impl namespace metric tags signed-val]
  This is the function to report a [counter](https://github.com/statsd/statsd/blob/master/docs/metric_types.md#counting) to statsd.
  Args:
  - impl :  the class object that implements this protocol, i.e. an instance of the deftype for example
  - namespace : the namespace of the metric e.g. `message-processing`
  - metric : the metric for which the value is being generated e.g. `success`, the final metric is created concatenating the namespace and the metric `namespace.metric` - `message-processing.success`
  - tags : these are the tags attached to each metric. They are passed in the form of a clojure map e.g. {:topic_entity \"stream\" :actor \"application\"} which can then be converted to statsd specific tags (see `ziggurat.clj-statsd-metrics-wrapper` namespace for example)
  - signed-val : this is the value that the counter should be updated by. It is a signed value, so it will have a +ve or -ve value.

  Update-timing [impl namespace metric tags value]
  This is the function to report a [timing](https://github.com/statsd/statsd/blob/master/docs/metric_types.md#timing) to statsd.
  Args:
  - impl :  the class object that implements this protocol, i.e. an instance of the deftype for example
  - namespace : the namespace of the metric e.g. `message-processing`
  - metric : the metric for which the value is being generated, currently a constant metric name - `all` is being passed for all update-timing. This is due to legacy reasons. We can't change it suddenly as it will be a breaking change for current applications and they will need to change their dashboards.
  - tags : these are the tags attached to each metric. They are passed in the form of a clojure map e.g. {:topic_entity \"stream\" :actor \"application\"} which can then be converted to statsd specific tags (see `ziggurat.clj-statsd-metrics-wrapper` namespace for example)
  - value : the value which is to be reported for the timing.
  "
  (initialize [impl statsd-config])
  (terminate  [impl])
  (update-counter [impl namespace metric tags signed-val])
  (update-timing [impl namespace metric tags value]))


