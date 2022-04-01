# KVRaft

## To run/test Milestone 1

### Run coord and servers for server join process
1. Start tracing server
```console
go run cmd/tracing-server/main.go
```

2. Run coord
```console
go run cmd/coord/main.go
```
3. Run servers, where `{server-id}` is id of current server.
```console
go run cmd/server/main.go {server-id}
```
Like A3, to start N servers, we run the above command on separate terminals, where {server-id} goes from 1 to N. The current config file `config/coord_config.json` specifies N=3 servers with `"NumServers"` field, but can start N <= 7 servers as specified by proposal, where the number of servers are odd.

#### Sample output of Join Process (With N = 3 and current config files)
##### Coord
```
Join process completed; Leader is {1 127.0.0.1:21001 127.0.0.1:21001 127.0.0.1:21021 127.0.0.1:21031 [167 115 101 114 118 101 114 49 207 100 61 169 50 180 138 37 247 129 167 115 101 114 118 101 114 49 4]}
serverClusterView: [{1 127.0.0.1:21001 127.0.0.1:21001 127.0.0.1:21021 127.0.0.1:21031 [167 115 101 114 118 101 114 49 207 100 61 169 50 180 138 37 247 129 167 115 101 114 118 101 114 49 4]} {2 127.0.0.1:21002 127.0.0.1:21002 127.0.0.1:21022 127.0.0.1:21032 [167 115 101 114 118 101 114 50 207 27 83 250 226 65 65 217 247 129 167 115 101 114 118 101 114 50 4]} {3 127.0.0.1:21003 127.0.0.1:21003 127.0.0.1:21023 127.0.0.1:21033 [167 115 101 114 118 101 114 51 207 72 6 61 247 249 22 13 251 129 167 115 101 114 118 101 114 51 4]}]
remote fchecker addresses:  map[1:127.0.0.1:46251 2:127.0.0.1:36698 3:127.0.0.1:51591]
local fchecker addresses:  map[1:127.0.0.1:51921 2:127.0.0.1:49742 3:127.0.0.1:58377]
```
Coord sets first server (server-id=1) as leader, and updates its serverClusterView to include all three servers, and begins monitoring the servers in the cluster. To verify that the monitoring of servers is correct, one can check the generated log file `LOGS-FCHECK-{EpochNonce}.log`.

##### Server (leader output)
```
I'm a leader.
Peers: [{1 127.0.0.1:21001 127.0.0.1:21001 127.0.0.1:21021 127.0.0.1:21031 [167 115 101 114 118 101 114 49 207 100 61 169 50 180 138 37 247 129 167 115 101 114 118 101 114 49 4]} {2 127.0.0.1:21002 127.0.0.1:21002 127.0.0.1:21022 127.0.0.1:21032 [167 115 101 114 118 101 114 50 207 27 83 250 226 65 65 217 247 129 167 115 101 114 118 101 114 50 4]} {3 127.0.0.1:21003 127.0.0.1:21003 127.0.0.1:21023 127.0.0.1:21033 [167 115 101 114 118 101 114 51 207 72 6 61 247 249 22 13 251 129 167 115 101 114 118 101 114 51 4]}]
 TermNumber: 1
2022/04/01 13:14:46 [server1] TraceID=7223115412614555127 ServerJoined ServerId=1
```
### To test kvs client:

Run command 
```console
go run cmd/client/main.go”
````
Should initialize a client instance that currently has a connection to the leader node from communicating with the coord

