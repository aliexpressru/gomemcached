# Gomemcached

---
<div align=center>
<img src="https://github.com/aliexpressru/gomemcached/raw/main/assets/logo.png" width="300"/>

[![License](https://img.shields.io/github/license/gogf/gf.svg?style=flat)](https://github.com/aliexpressru/gomemcached)
[![Tag](https://img.shields.io/github/v/tag/aliexpressru/gomemcached?color=%23ff8936&logo=fitbit)](https://github.com/aliexpressru/gomemcached/tags)
[![Godoc](https://godoc.org/github.com/aliexpressru/gomemcached?status.svg)](https://pkg.go.dev/github.com/aliexpressru/gomemcached)

[![Gomemcached](https://goreportcard.com/badge/github.com/aliexpressru/gomemcached)](https://goreportcard.com/report/github.com/aliexpressru/gomemcached)
![Coverage](https://img.shields.io/badge/Coverage-93.6%25-brightgreen)

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go?tab=readme-ov-file#nosql-database-drivers)
</div>

___
`Gomemcached` is a Golang Memcached client designed to interact with multiple instances as shards. Implements sharding using a Consistent Hash.
___

### Configuration

Configuration is primarily done through environment variables.

#### Namespace

By default, the client uses the **"aer"** namespace for environment variables and metrics. This means:

**Environment variables:**
```yaml
    - name: AER_MEMCACHED_HEADLESS_SERVICE_ADDRESS
      value: "my-memchached-service-headless.namespace.svc.cluster.local"
    - name: AER_MEMCACHED_PORT
      value: "11211"
    - name: AER_MEMCACHED_SERVERS
      value: "127.0.0.1:11211,192.168.0.1:1234"
```

#### Custom or Empty Namespace

You can customize or disable the namespace prefix using `WithNamespace()`:

```go
// Use no prefix (legacy behavior)
mcl, err := memcached.InitFromEnv(ctx, memcached.WithNamespace(""))
// Environment variables: MEMCACHED_SERVERS, MEMCACHED_PORT, MEMCACHED_HEADLESS_SERVICE_ADDRESS
// Metrics: gomemcached_method_duration_seconds, gomemcached_object_size_bytes

// Use custom prefix
mcl, err := memcached.InitFromEnv(ctx, memcached.WithNamespace("myapp"))
// Environment variables: MYAPP_MEMCACHED_SERVERS, MYAPP_MEMCACHED_PORT, MYAPP_MEMCACHED_HEADLESS_SERVICE_ADDRESS
// Metrics: myapp_gomemcached_method_duration_seconds, myapp_gomemcached_object_size_bytes
```

#### Configuration Variables

`MEMCACHED_HEADLESS_SERVICE_ADDRESS` (or `AER_MEMCACHED_HEADLESS_SERVICE_ADDRESS`) groups all memcached instances by ip addresses using dns lookup.

Default Memcached port is `11211`, but you can also specify it with `MEMCACHED_PORT` (or `AER_MEMCACHED_PORT`).

For local run or if you have a static amount and setup of pods you can specify Servers (list separated by commas along with the port) manually with `MEMCACHED_SERVERS` (or `AER_MEMCACHED_SERVERS`) instead of setting the HeadlessServiceAddress.

> **Note:** Environment variables are the preferred configuration method, but can be overridden programmatically with `WithHeadlessServiceAddress()`, `WithServersList()`, and `WithMemcachedPort()` options.
___

### Usage

Initialization client and connected to memcached servers.

```go
    mcl, err := memcached.InitFromEnv(ctx)
    mustInit(err)
    gracefulShutdown(
        func() error {
            mcl.CloseAllConns(ctx)
            return nil
        },
    )
```
[More examples](examples/usage_example.go)

To use SASL specify option for InitFromEnv:

```go
    memcached.InitFromEnv(memcached.WithAuthentication("<login>", "<password>"))
```

Can use Options with InitFromEnv to customize the client to suit your needs. However, for basic use, it is recommended
to use the default client implementation.

#### Metrics

The client automatically collects Prometheus metrics for method execution time and object sizes. By default, metrics are registered with `prometheus.DefaultRegisterer` and use the **"aer"** namespace prefix. See [metrics.go](memcached/metrics.go) for implementation details.

Available metrics (with default "aer" namespace):
- `aer_gomemcached_method_duration_seconds` - histogram of method execution times
- `aer_gomemcached_object_size_bytes` - histogram of object sizes being stored

##### Default Usage

Metrics are automatically registered with the default Prometheus registry and "aer" namespace:

```go
mcl, err := memcached.InitFromEnv(ctx)
// Metrics: aer_gomemcached_method_duration_seconds, aer_gomemcached_object_size_bytes
```

##### Custom Prometheus Registry

You can use a custom Prometheus registry for metric isolation or multi-tenancy:

```go
customRegistry := prometheus.NewRegistry()
mcl, err := memcached.InitFromEnv(
    ctx,
    memcached.WithMetricsRegisterer(customRegistry),
)
```

##### Custom Histogram Buckets

Adjust histogram buckets to match your expected latency profile:

```go
// Default duration buckets (seconds): [0.0005, 0.001, 0.005, 0.007, 0.015, 0.05, 0.1, 0.2, 0.5, 1]
customDurationBuckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0}
mcl, err := memcached.InitFromEnv(
    ctx,
    memcached.WithMetricsDurationBuckets(customDurationBuckets),
)
```

Adjust object size buckets to match your typical data sizes:

```go
// Default size buckets (bytes): [10, 100, 1024, 10240, 51200, 102400, 524288, 1048576, 5242880, 10485760]
// (10 bytes, 100 bytes, 1KB, 10KB, 50KB, 100KB, 512KB, 1MB, 5MB, 10MB)
customSizeBuckets := []float64{1024, 10240, 102400, 1048576}  // 1KB, 10KB, 100KB, 1MB
mcl, err := memcached.InitFromEnv(
    ctx,
    memcached.WithMetricsObjectSizeBuckets(customSizeBuckets),
)
```

##### Disable Metrics

To disable metrics collection entirely:

```go
memcached.InitFromEnv(ctx, memcached.WithDisableMemcachedDiagnostic())
```

See [custom_metrics_example.go](examples/custom_metrics_example.go) for more examples.

---

### Recommended Versions

This project is developed and tested with the following recommended versions:

- Go: 1.24 or higher
   - [Download Go](https://golang.org/dl/)

- Memcached: 1.6.9 or higher
   - [Memcached Releases](https://memcached.org/downloads)

--- 

### Dependencies

This project utilizes the following third-party libraries, each governed by the MIT License:

1. [gomemcache](https://github.com/bradfitz/gomemcache)
    - Description: A Go client library for the memcached server.
    - Used for: Primary client methods for interacting with the library.
    - License: Apache License 2.0

2. [go-zero](https://github.com/zeromicro/go-zero)
    - Description: A cloud-native Go microservices framework with cli tool for productivity.
    - Used for: Implementation of Consistent Hash.
    - License: MIT License

3. [gomemcached](https://github.com/dustin/gomemcached)
    - Description: A memcached binary protocol toolkit for go.
    - Used for: Implementation of a binary client for Memcached.
    - License: MIT License

Please review the respective license files in the linked repositories for more details.
