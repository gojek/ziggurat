## Connecting to a RabbitMQ cluster for retries

- To connect to RabbitMQ clusters add the following config to your `config.edn`

```clojure
{:ziggurat {:messaging {:constructor "ziggurat.messaging.rabbitmq-cluster-wrapper/->RabbitMQMessaging"
            :rabbit-mq-connection {:hosts "g-lambda-lambda-rabbitmq-a-01,g-lambda-lambda-rabbitmq-a-02,g-lambda-lambda-rabbitmq-a-03"
                                   :port [5672 :int]
                                   :prefetch-count  [3 :int]
                                   :username        "guest"
                                   :password        "guest"
                                   :channel-timeout [2000 :int]
                                   :address-resolver [:dns :keyword] ;;possible values [:dns :ip-list]. Default is :dns
                                   :channel-pool     {:max-wait-ms [5000 :int]
                                                      :min-idle    [10 :int]
                                                      :max-idle    [20 :int]}
                                   :publish-retry   {:back-off-ms 5000
                                                     :non-recoverable-exception {:enabled true
                                                                                 :back-off-ms 1000
                                                                                 :count 5}}}}}}
```

- `:hosts` is a comma separated values of RabbitMQ hostnames (dns-names OR IPs).
- `:port` specifies the port number on which the RabbitMQ nodes are running.
- `:prefetch-count` Sets the prefetch count for RabbitMQ, determining the number of messages that can be consumed from a channel before an acknowledgment is received. 
  - The value 3 means up to 3 messages can be prefetched.
  - The prefetch count is per worker. so, the prefetch-count mentioned here is for each worker in worker-count.
- `:channel-pool` specifies the properties for the RabbitMQ channel pool used for publishing
- `:address-resolver` specifies the strategy to figure out RabbitMQ hosts IP addresses. `:dns` is the default and shoud
  be used when `:hosts` specifies a DNS address. `:ip-list` should be used when comma separated IPs are provided.
- `:publish-retry` defines the config for recoverable and non-recoverable exceptions.
    - Recoverable exceptions
        - `:back-off-ms` - defines the time period after which a retry should happen
    - Non-recoverable exceptions
        - `:enabled` - defines whether retries should happen
        - `:back-off-ms` - defines the time period after which a retry should happen
        - `:count` - defines the number of retries
- By default, your queues and exchanges are replicated across (n+1)/2 nodes in the cluster

## Exponential Backoff based Retries

In addition to linear retries, Ziggurat users can now use exponential backoff strategy for retries. This means that the message
timeouts after every retry increase by a factor of 2. So, if your configured timeout is 100ms the backoffs will have timeouts as
`200, 300, 700, 1500 ..`. These timeouts are calculated using the formula `(queue-timeout-ms * ((2**exponent) - 1))` where `exponent` falls in this range `[1,(min 25, configured-retry-count)]`.

The number of retries possible in this case are capped at 25.

The number of queues created in the RabbitMQ are equal to the configured-retry-count or 25, whichever is smaller.

Exponential retries can be configured as described below.

```$xslt
:ziggurat {:stream-router {:default {:application-id "application_name"...}}}
           :retry         {:type   [:exponential :keyword]
                           :count  [10 :int]
                           :enable [true :bool]}

```

Exponential retries can be configured for channels too. Additionally, a user can specify a custom `queue-timeout-ms` value per channel.
Timeouts for exponential backoffs are calculated using `queue-timeout-ms`. This implies that each channel can have separate count of retries
and different timeout values.

```$xslt
:ziggurat {:stream-router {:default {:application-id "application_name"...
                                     :channels {:channel-1 .....
                                                           :retry {:type   [:exponential :keyword]
                                                                                      :count  [10 :int]
                                                                                      :queue-timeout-ms 2000
                                                                                      :enable [true :bool]}}}}}
```

## Channels

Channels enable you to increase the number of parallel processors more than the number of partitions of your topic.
Messages consumed from topics are directly sent to rabbitmq channels and the mapper function handler processes messages from this channel.
You can set the worker count as per your parallel processing requirement. The channel configs are described below in configuration section.

```$xslt
:ziggurat {:stream-router {:stream-id {:application-id "application_name"...
                                     :channels {:channel-1 {:worker-count [10 :int]
                                                            :retry        {:type    [:linear :keyword]
                                                                           :count   [5 :int]
                                                                           :enabled [true :bool]}}}
```

How to send messages directly to channel after consuming.
```clojure
:stream-routes {:stream-id {:handler-fn   (fn [_] :channel-1)
                               :channel-1 (my-protobuf-hash handler-fn)}}

```

The above method creates an anonymous function that passes the consumed messages to channel and the channel route is then handled by the handler-fn
you have created.