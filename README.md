# mini-redis

Learn redis internal by building a mini-version of it. This little program actually runs surprisingly fast :D

Support Commands:

- PING
- GET
- SET
- DEL
- INCR
- DECR
- EXPIRE
- PEXPIRE
- TTL
- PTTL

mini-redis spawns multiple goroutines for I/O, but only uses one goroutine to execute command.

Relevant Links:

- https://github.com/redis/redis-specifications/blob/master/protocol/RESP2.md
- https://redis.io/docs/reference/protocol-spec/
- https://redis.io/commands/
