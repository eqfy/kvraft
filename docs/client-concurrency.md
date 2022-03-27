# Client Concurrency

There are 3 major constraints in our system arising from the OpId and Gid consistency semantics. They are

### 1a. Put-Get Data Consistency
- Given any:
  - Put(_, _, key, value1) and a corresponding PutResultRecvd(_, gId1, key), and
  - GetResultRecvd(_, gId3, key, value3)
- Such that, if:
  - gId1 < gId3
  - And, ∄ PutResultRecvd(_, gId2, key) such that gId1 < gId2 < gId3
- Then
  - value3 == value1


### 1b. Get before any Put Data Consistency
- Given any
  - GetResultRecvd(_, gId2, key, value1)
- If
  - ∄ PutResultRecvd(_, gId1, key) such that gId1 < gId2
- Then
  - value1 == ""


### 2. OpID consistent with gID
- Let cId be a client id
- Let Req = { op | [ op = Put(clientId, _, _, _) ∨ op = Get(clientId', _,_)] ⋀ clientId = clientId' = cId }
- Let Res = { op | [ op = PutResultRecvd(clientId, _,_) ∨ op = GetResultRecvd(clientId', _, _ ,_)] ⋀ clientId = clientId' = cId }
- Let op1Req ∈ Req have opID opId1
- Let op2Req ∈ Req, op2Req ≠ op1Req, have opID opId2,
- Let op1Res ∈ Res be in the same trace as op1Req and have gID gId1
- Let op2Res ∈ Res be in the same trace as op2Req and have gID gId2
- Then,
  - gId1 < gId2 ⇔ opId1 < opId2, and
  - gId1 > gId2 ⇔ opId1 > opId2

### 3. Opid must increase monotonically

## Implications:
Due to our gid design, in order to achieve 2, we must have send gets only after puts are done. Unless we are able to skip opids (which would violate 3), the only concurrency that can achieve 2, 3 is by only allowing concurrent gets OR concurrent puts. We cannot have concurrent get and puts




can the client stop and restart again? I think we are assuming no!