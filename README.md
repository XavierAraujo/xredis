# xRedis — A Minimal Redis Clone in Go

This is a simplified Redis server implemented in Golang as part of the [John Cricket Redis coding challenges](https://codingchallenges.fyi/challenges/challenge-redis/). It supports a subset of Redis commands and is compatible with the [redis-cli](https://redis.io/docs/latest/develop/tools/cli/).

## 🚀 Features

- Basic Redis TCP server
- Command support:
  - `PING`
  - `ECHO`
  - `GET`
  - `SET`
  - `LPUSH`
  - `RPUSH`
  - `INCR`
  - `DECR`
  - `SET` with `EX`,`PX`,`EXAT`,`PXAT` (timeout/expiry support)

---

## 🛠 How to run

### 1. Clone the repository

```bash
git clone https://github.com/XavierAraujo/xredis.git
```

### 2. Build the project and run the compiled binary
```
cd xredis
go build
./xredis
```

## 💬 Interacting with the Server
You can use the official redis-cli tool to interact with your GoRedis server:

redis-cli -p 6379
```bash
# PING
127.0.0.1:6379> PING
PONG

# ECHO
127.0.0.1:6379> ECHO "Hello"
"Hello"

# SET and GET
127.0.0.1:6379> SET mykey "GoLang"
OK
127.0.0.1:6379> GET mykey
"GoLang"

# SET with EX (expiry in seconds)
127.0.0.1:6379> SET temp "data" EX 5
OK
127.0.0.1:6379> GET temp
"data"
# Wait 5 seconds...
127.0.0.1:6379> GET temp
(nil)

# INCR and DECR
127.0.0.1:6379> SET counter 10
OK
127.0.0.1:6379> INCR counter
(integer) 11
127.0.0.1:6379> DECR counter
(integer) 10

# LPUSH and RPUSH
127.0.0.1:6379> LPUSH mylist "one"
(integer) 1
127.0.0.1:6379> RPUSH mylist "two"
(integer) 2
```