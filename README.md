# KVRaft
## To run and test milestone 2
## Running the KVRaft service 

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
To start N servers, we run the above command on separate terminals, where {server-id} goes from 1 to N. The current config file `config/coord_config.json` specifies N=5 servers with `"NumServers"` field. Can set N <= 7 servers as specified by proposal, where the number of servers are odd.

4. Run client
```console
go run cmd/client/main.go {client-id}
````
Where `{client-id}` is the ID of the current Client. This should initialize a client instance that currently has a connection to the leader node from communicating with the coord

Make sure to have all servers joined before running as the client will block until this condition is met. Run a combination of get and put requests with the syntax below in cmd/client/main.go:

```console
client.Get(key_name)
client.Put(key_name, key_value)
````

### Visualization

Download the generated `shiviz_output.log` file and to visualize it with https://bestchai.bitbucket.io/shiviz/

## Testing each component on its own

### Coord (network partitions)
To test this, uncomment the following calls in server.go:
```
mockLogs(s)
```
and 
```
simulateNetworkPartition(s)
```
Then run the coord, servers and the tracing server. The current test setup requires a system of 5 servers. We simulate a network partition by having a subset of servers(the current leader(node 1) and a follower(node 5)) not respond to the heartbeats sent by the coord for some time. The coord detects this subset as failed and selects the next most up-to-date node(node 4) as the new leader. When the simulated network partition heals, the coord can communicate with nodes 1 and 5 and then updates its cluster view.

The two functions above could be modified to test different variants of network partitions and/or leader-selection.
### Log replication

Server (raft and log replication) was tested by itself before integrating with client, with the test file `cmd/server/test/main.go`. Run tracing server, coord, and servers as above, and then run `go run cmd/server/test/main.go`. 
#### Expected output
```
Test 1 Put Passed with key=1, val=10 
2022/04/08 20:22:44 [client1] TraceID=5995138230248986026 GenerateTokenTrace Token=[167 99 108 105 101 110 116 49 207 83 51 2 116 241 90 213 170 129 167 99 108 105 101 110 116 49 3]
Test 1 Put Passed with key=2, val=20 
2022/04/08 20:22:44 [client1] TraceID=5995138230248986026 GenerateTokenTrace Token=[167 99 108 105 101 110 116 49 207 83 51 2 116 241 90 213 170 129 167 99 108 105 101 110 116 49 4]
Test 1 Put Passed with key=3, val=30 
2022/04/08 20:22:44 [client1] TraceID=5995138230248986026 GenerateTokenTrace Token=[167 99 108 105 101 110 116 49 207 83 51 2 116 241 90 213 170 129 167 99 108 105 101 110 116 49 5]
...
Test 1 Get Passed: expected value: 10, got 10 
2022/04/08 20:22:45 [client1] TraceID=5995138230248986026 GenerateTokenTrace Token=[167 99 108 105 101 110 116 49 207 83 51 2 116 241 90 213 170 129 167 99 108 105 101 110 116 49 13]
Test 1 Get Passed: expected value: 20, got 20 
2022/04/08 20:22:45 [client1] TraceID=5995138230248986026 GenerateTokenTrace Token=[167 99 108 105 101 110 116 49 207 83 51 2 116 241 90 213 170 129 167 99 108 105 101 110 116 49 14]
Test 1 Get Passed: expected value: 30, got 30 
2022/04/08 20:22:45 [client1] TraceID=5995138230248986026 GenerateTokenTrace Token=[167 99 108 105 101 110 116 49 207 83 51 2 116 241 90 213 170 129 167 99 108 105 101 110 116 49 15]
...
```
