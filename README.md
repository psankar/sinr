# sinr

Sinr Is Not Redis

A re-implementation of Redis using Golang. Currently supports GET and SET (with all parameters).

## Usage
```
$ go run main.go
$ redis-cli -p 15000
redis> SET key val [EX seconds] [PX milliseconds] [NX|XX]
redis> GET key

$ redis-benchmark -p 15000 -t SET,GET -n 10000
```

In my macbook pro, the Go version handles about 65500 requests per second, while the original, performance optimized upstream redis handles about 77000 requests per second. Not bad for safe, managed code, without any performance optimization.