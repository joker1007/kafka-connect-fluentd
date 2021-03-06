# Kafka Connect Fluentd Connector

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Ffluent%2Fkafka-connect-fluentd.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Ffluent%2Fkafka-connect-fluentd?ref=badge_shield)
[![Build Status](https://travis-ci.org/fluent/kafka-connect-fluentd.svg?branch=master)](https://travis-ci.org/fluent/kafka-connect-fluentd)

kafka-connect-fluentd is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for copying data between Kafka and [Fluentd](https://www.fluentd.org/).
kafka-connect-fluentd can be alternative to [fluent-plugin-kafka](https://github.com/fluent/fluent-plugin-kafka).

## Installation

You can download and install kafka-connect-fluentd from [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22kafka-connect-fluentd%22).

For more details, see [Installing Plugins](https://docs.confluent.io/current/connect/userguide.html#installing-plugins).

## Running Kafka and Kafka Connector Workers

See [Running Workers](https://docs.confluent.io/current/connect/userguide.html#running-workers).

Run Apache Kafka:

```
(on terminal 1)
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties 
(on terminal 2)
$ ./bin/kafka-server-start.sh config/server.properties
```

Run kafka-connect-fluentd (FluentdSourceConnector/FluentdSinkConnector):

```
(on terminal 3)
$ bin/connect-standalone.sh config/connect-standalone.properties \
    /path/to/kafka-connect-fluentd/config/FluentdSourceConnector.properties \
    /path/to/kafka-connect-fluentd/config/FluentdSinkConnector.properties
```

**NOTE:** Copy jar file to `CLASSPATH` or change `plugin.path` in connect-standalone.properties.
Use same `topics` in FluentdSourceConnector.properties and FluentdSinkConnector.properties.

FluentdSourceConnector.properties:

```
name=FluentdSourceConnector
tasks.max=1
connector.class=org.fluentd.kafka.FluentdSourceConnector
fluentd.port=24224
fluentd.bind=0.0.0.0

fluentd.worker.pool.size=1
fluentd.counter.enabled=true
```

FluentdSinkConnector.properties:

```
name=FluentdSinkConnector
topics=fluentd-test
tasks.max=1
connector.class=org.fluentd.kafka.FluentdSinkConnector
fluentd.connect=localhost:24225
```

Setup Fluentd:

See also [Fluentd Installation](https://docs.fluentd.org/v1.0/categories/installation).

```
(on terminal 4)
$ git clone https://github.com/fluent/fluentd.git
$ cd fluentd
$ bundle install
```

Run Fluentd:

```
(on terminal 4)
$ bundle exec fluentd -c fluent.conf
```

fluent.conf:

```aconf
<source>
  @type forward
  port 24225
</source>
<match fluentd-test>
  @type stdout
</match>
```

And emit records:

```
(on terminal 5)
$ cd fluentd
$ echo '{"messmages": "Hi, Kafka connect!"}' | bundle exec fluent-cat fluentd-test --time-as-integer
```

**NOTE:** Specify tag same as topics in FluentdSourceConnector.properties and FluentdSinkConnector.properties.

See also [kafka-connect benchmark scripts](https://github.com/fluent/fluentd-benchmark/tree/master/kafka-connect).

### Configuration for FluentdSourceConnector

See also [Influent](https://github.com/okumin/influent).

* fluentd.port
  * Port number to listen. Default: `24224`
* fluentd.bind
  * Bind address to listen. Default: `0.0.0.0`
* fluentd.chunk.size.limit
  * Allowable chunk size. Default: `Long.MAX_VALUE`
* fluentd.backlog
  * The maximum number of pending connections for a server. Default: `0`
* fluentd.send.buffer.bytes
  * `SO_SNDBUF` for forward connection. `0` means system default value. Default: `0`
* fluentd.receve.buffer.bytes
  * `SO_RCVBUF` for forward connection. `0` means system default value. Default: `0`
* fluentd.keep.alive.enabled
  * If `true`, `SO_KEEPALIVE` is enabled. Default: `true`
* fluentd.tcp.no.delay.enabled
  * If `true`, `TCP_NODELAY` is enabled. Default: `true`
* fluentd.worker.pool.size
  * Event loop pool size. `0` means auto. Default: `0`
* fluentd.transport
  * Set Fluentd transport protocol to `tcp` or `tls`. Default: `tcp`
* fluentd.tls.versions
  * TLS version. `TLS`, `TLSv1`, `TLSv1.1` or `TLSv1.2`. Default: `TLSv1.2`
* fluentd.tls.ciphers
  * Cipher suites
* fluentd.keystore.path
  * Path to keystore
* fluentd.keystore.password
  * Password for keystore
* fluentd.key.password
  * Password for key
* kafka.topic
  * Topic for Kafka. `null` means using Fluentd's tag for topic dynamically. Default: `null`
* fluentd.schemas.enable
  * Enable schemas for messages. Default: `true`
* fluentd.counter.enabled
  * **For developer only** Enable counter for messages/sec. Default: `false`

### Configuration for FluentdSinkConnector

See also [Fluency](https://github.com/komamitsu/fluency).

* fluentd.connect
  * Connection specs for Fluentd. Default: localhost:24224
* fluentd.client.max.buffer.bytes
  * Max buffer size.
* fluentd.client.buffer.chunk.initial.bytes
  * Initial size of buffer chunk. Default: 1048576 (1MiB)
* fluentd.client.buffer.chunk.retention.bytes
  * Retention size of buffer chunk. Default: 4194304 (4MiB)
* fluentd.client.flush.interval
  * Buffer flush interval in msec. Default: 600(msec)
* fluentd.client.ack.response.mode
  * Enable/Disable ack response mode. Default: false
* fluentd.client.file.backup.dir
  * Enable/Disable file backup mode. Default: false
* fluentd.client.wait.until.buffer.flushed
  * Max wait until all buffers are flushed in sec. Default: 60(sec)
* fluentd.client.wait.until.flusher.terminated
  * Max wait until the flusher is terminated in sec. Default: 60(sec)
* fluentd.client.jvm.heap.buffer.mode
  * If true use JVM heap memory for buffer pool. Default: false
* fluentd.client.timestamp.integer
  * If true, use integer timestamp (unix timestamp). Default: false

NOTE: Fluency doesn't support SSL/TLS yet

### Example of SSL/TLS support with Fluentd

FluentdSourceConnector.properties

```
name=FluentdSourceConnector
tasks.max=1
connector.class=org.fluentd.kafka.FluentdSourceConnector
fluentd.port=24224
fluentd.bind=0.0.0.0
fluentd.transport=tls
fluentd.keystore.path=/path/to/influent-server.jks
fluentd.keystore.password=password-for-keystore
fluentd.key.password=password-for-key
```

fluent.conf

```aconf
<source>
  @type dummy
  dummy {"message": "this is test"}
  tag test
</source>

<filter test>
  @type stdout
</filter>
<match test>
  @type forward
  transport tls
  tls_cert_path /path/to/ca_cert.pem
  # tls_verify_hostname false # for test
  heartbeat_type none
  <server>
    # first server
    host 127.0.0.1
    port 24224
  </server>
  <buffer>
    flush_interval 1
  </buffer>
</match>
```

Run kafka-connect-fluentd and then run Fluentd with above configuration:

```text
(on terminal 1)
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties
(on terminal 2)
$ ./bin/kafka-server-start.sh config/server.properties
(on terminal 3)
$ bin/connect-standalone.sh config/connect-standalone.properties \
    /path/to/kafka-connect-fluentd/config/FluentdSourceConnector.properties \
    /path/to/connect-file-sink.properties
(on terminal 4)
$ fluentd -c fluent.conf
```

## License

Apache License, Version 2.0
