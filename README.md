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
> PING
PONG

# ECHO
> ECHO "Hello"
"Hello"

# SET and GET
> SET mykey "GoLang"
OK
> GET mykey
"GoLang"

# EXISTS
> EXISTS mykey
(integer) 1
> EXISTS nonexisting
(integer) 0

# DEL
> DEL mykey
(integer) 1
> GET mykey
(nil)

# SET with EX (expiry in seconds)
> SET temp "data" EX 5
OK
> GET temp
"data"
# Wait 5 seconds...
> GET temp
(nil)

# INCR and DECR
> SET counter 10
OK
> INCR counter
(integer) 11
> DECR counter
(integer) 10

# LPUSH and RPUSH
> LPUSH mylist "one"
(integer) 1
> RPUSH mylist "two"
(integer) 2
```


## 🧪 How to test

Just run

```bash
go test
```