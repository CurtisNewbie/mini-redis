# mini-redis

Learn redis internal by building a mini-version of it.

Support Commands:

- PING
- GET
- SET
- INCR
- DECR
- DEL

mini-redis spawns multiple goroutines for I/O, but only uses one goroutine to execute command.