# Server Design

Assuming:
- Server: S1 -> S2 -> S3 -> S4
- KVS: K1, K2, K3
- Coord: C


Server needs:
- Pending - a list of put requests unprocessed by the server
- For some server s:
  - Pending(s) is the list of put requests unprocessed by the tail server according to s

## Head server failure
- s1 failed, new head is s2
- Pending(s1) >= Pending(s2)
- Client resend Pending(s1) - Pending (s2)
  - Client does this by waiting for either
    - a timeout (maybe not necessary)
    - a change head request finishes


## Tail server failure
- s4 fails, new tail is s1



## Middle server failure
**Must be handled by the servers themselves**
- reason is that otherwise, client resends may not be in order of 


## TODO
Server side
1. Get reverse forwarding - forward up the chain the latest get gid processed by tail
2. Put reverse forwarding - forward up the chain the latest put request processed by tail
3. Confirm that each server has updated KVS when the they do the put forward
4. 1 - 3 to handle middle server failure

Client side
1. investiate strategic usage of opids
2. send put and get concurrently