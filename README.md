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

## Testing server failure

The system can survive up to N/2 - 1 failures. To test for server failure, can terminate servers (fail-stop failure). If a leader fails, then coord will designate a new leader, and kvslib will failover to the new leader. 

### Example output
Testing with 20 puts and gets as specified in `cmd/client/main.go`. 
1. Fail server 1 (leader), which fails after sending request Put(k13,val13) at log index 27
```
(Leader Put): received client command={{1 k13 val13 {1 27}} 0xc0003b70e0}
(Leader AppendEntries): Sending AppendEntries={1 1 26 1 [{1 Put(k13,val13) 27}] 26 []} to followers.
2022/04/15 21:02:51 [server1] TraceID=5400200269340830504 CreateTrace
2022/04/15 21:02:51 [server1] TraceID=6000395523022580870 CreateTrace
2022/04/15 21:02:51 [server1] TraceID=6000395523022580870 AppendEntriesRequestSent Term=1, LeaderId=1, PrevLogIndex=26, PrevLogTerm=1, Entries=[{1 Put(k13,val13) 27}], LeaderCommit=26, Token=[]
2022/04/15 21:02:51 [server1] TraceID=6000395523022580870 GenerateTokenTrace Token=[167 115 101 114 118 101 114 49 207 83 69 175 239 205 154 236 134 133 167 99 108 105 101 110 116 49 205 9 125 167 115 101 114 118 101 114 49 205 14 253 165 99 111 111 114 100 205 1 18 167 115 101 114 118 101 114 50 205 23 51 167 115 101 114 118 101 114 51 205 15 54]
2022/04/15 21:02:51 [server1] TraceID=5400200269340830504 AppendEntriesRequestSent Term=1, LeaderId=1, PrevLogIndex=26, PrevLogTerm=1, Entries=[{1 Put(k13,val13) 27}], LeaderCommit=26, Token=[]
2022/04/15 21:02:51 [server1] TraceID=5400200269340830504 GenerateTokenTrace Token=[167 115 101 114 118 101 114 49 207 74 241 93 136 146 7 107 40 133 167 115 101 114 118 101 114 49 205 14 255 165 99 111 111 114 100 205 1 18 167 115 101 114 118 101 114 50 205 23 51 167 115 101 114 118 101 114 51 205 15 54 167 99 108 105 101 110 116 49 205 9 125]
2022/04/15 21:02:51 [server1] TraceID=5400200269340830504 ReceiveTokenTrace Token=[167 115 101 114 118 101 114 50 207 74 241 93 136 146 7 107 40 133 165 99 111 111 114 100 205 1 18 167 115 101 114 118 101 114 49 205 14 255 167 115 101 114 118 101 114 51 205 15 54 167 99 108 105 101 110 116 49 205 9 125 167 115 101 114 118 101 114 50 205 23 55]
2022/04/15 21:02:51 [server1] TraceID=5400200269340830504 AppendEntriesResponseRecvd Term=1, Success=true, Token=[167 115 101 114 118 101 114 50 207 74 241 93 136 146 7 107 40 133 165 99 111 111 114 100 205 1 18 167 115 101 114 118 101 114 49 205 14 255 167 115 101 114 118 101 114 51 205 15 54 167 99 108 105 101 110 116 49 205 9 125 167 115 101 114 118 101 114 50 205 23 55]
^Csignal: interrupt
```
2. Coord detects failure
```
ServerClusterView: 2, 3, 
New leader: 2
```
3. Server 2 becomes new leader
```
Received leader failure notification. I'm the new leader.
2022/04/15 21:02:51 [server2] TraceID=5908848541286080979 ReceiveTokenTrace Token=[165 99 111 111 114 100 207 82 0 114 113 30 241 33 211 133 167 115 101 114 118 101 114 51 205 14 204 167 99 108 105 101 110 116 49 205 8 246 165 99 111 111 114 100 205 1 23 167 115 101 114 118 101 114 49 205 13 137 167 115 101 114 118 101 114 50 205 22 201]
2022/04/15 21:02:51 [server2] TraceID=5908848541286080979 ServerFailRecvd FailedServerId=1
2022/04/15 21:02:51 [server2] TraceID=4778927783780588014 NoOpRequest
2022/04/15 21:02:51 [server2] TraceID=5908848541286080979 GenerateTokenTrace Token=[167 115 101 114 118 101 114 50 207 82 0 114 113 30 241 33 211 133 167 115 101 114 118 101 114 49 205 14 255 167 115 101 114 118 101 114 51 205 15 54 167 99 108 105 101 110 116 49 205 9 125 167 115 101 114 118 101 114 50 205 23 59 165 99 111 111 114 100 205 1 23]
2022/04/15 21:02:51 [server2] TraceID=4778927783780588014 BecameLeader ServerId=2
(Leader AppendEntries): Sending AppendEntries={2 2 27 1 [{2 No-Op 28}] 26 []} to followers.
```
4. Client sends future requests to server 2 (i.e., requests onwards from Get(k13)).
5. Observe consistent logs on server 3 during fail over
```
...
25. Term=1, Index=25, Entry=Put(k12,val12)
26. Term=1, Index=26, Entry=Get(k12)
27. Term=1, Index=27, Entry=Put(k13,val13)
28. Term=2, Index=28, Entry=No-Op
29. Term=2, Index=29, Entry=Get(k13)
30. Term=2, Index=30, Entry=Put(k14,val14)
31. Term=2, Index=31, Entry=Get(k14)
32. Term=2, Index=32, Entry=Put(k15,val15)
33. Term=2, Index=33, Entry=Get(k15)
34. Term=2, Index=34, Entry=Put(k16,val16)
35. Term=2, Index=35, Entry=Get(k16)
...
```
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
