# McShredder

Shredder's Revenge is a great videogame. McShredder is a high speed workload
emulation and benchmark tool for memcached. It is a descendent of mc-crusher

- Lua scripting for easily emulating client workloads
- Scales to many client connections
- Mixing of workloads in a single test
- Easy workload rate limiting
- Multi-threading for scaling workloads
- `io_uring` network loop

This is an early release of the tool, with an "early and often" mantra. See
issues on github to track planned work or contribute.

## Dependencies

Most dependencies are vendored within the source.

A copy of liburing 2.1 must be symlinked into vendor/
(a compile bug exists in newer versions, bundling may happen at a later date)

Requies a Linux kernel within the last two years, for `io_uring` features.

## Building

Download and symlink or mv a copy of liburing 2.1 into vendor/liburing
`make`

Sorry about the bare makefile; low priority :)

## Start options

```
--conf [file.lua]
 - The test configuration to load/run.

--ip [127.0.0.1]
 - IP address to connect to

--port [11211]
 - port to connect to

## Configuration

See conf/example.lua for an introduction to the configuration language.

Please keep in mind: This is a benchmark tool, and as such we try to minimize
overhead. There is minimal argument checking, minimal error handling, etc. Try
to keep configuration files straightforward and simple.
