# Gomemcached

---
<div align=center>
<img src="https://github.com/aliexpressru/gomemcached/raw/main/assets/logo.png" width="300"/>

[![License](https://img.shields.io/github/license/gogf/gf.svg?style=flat)](https://github.com/aliexpressru/gomemcached)
[![Gomemcached](https://goreportcard.com/badge/github.com/aliexpressru/gomemcached)](https://goreportcard.com/report/github.com/aliexpressru/gomemcached)
[![Godoc](https://godoc.org/github.com/aliexpressru/gomemcached?status.svg)](https://pkg.go.dev/github.com/aliexpressru/gomemcached)
</div>

___
`Gomemcached` is a Golang Memcached client designed to interact with multiple instances as shards. Implements sharding using a Consistent Hash.
___

### Configuration

```yaml
    - name: MEMCACHED_HEADLESS_SERVICE_ADDRESS
      value: "my-memchached-service-headless.namespace.svc.cluster.local"
```

`MEMCACHED_HEADLESS_SERVICE_ADDRESS` groups all memcached instances by ip addresses using dns lookup.

Default Memcached port is `11211`, but you can also specify it in config.

```yaml
    - name: MEMCACHED_PORT
      value: "12345"
```

For local run or if you have a static amount and setup of pods you can specify Servers (list separated by commas along with the port) manually instead of setting the
HeadlessServiceAddress:

```yaml
  - name: MEMCACHED_SERVERS
    value: "127.0.0.1:11211,192.168.0.1:1234"
```

___

### Usage

Initialization client and connected to memcached servers.

```go
    mcl, err := memcached.InitFromEnv()
    mustInit(err)
    a.AddCloser(func () error {
        mcl.CloseAllConns()
        return nil
    })
```
[More examples](examples/main.go)

To use SASL specify option for InitFromEnv:

```go
    memcached.InitFromEnv(memcached.WithAuthentication("<login>", "<password>"))
```

Can use Options with InitFromEnv to customize the client to suit your needs. However, for basic use, it is recommended
to use the default client implementation.

---

### Recommended Versions

This project is developed and tested with the following recommended versions:

- Go: 1.18 or higher
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
