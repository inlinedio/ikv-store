# IKV-Store | Inlined.io Key Value Store
IKV-Store is a **fully-managed embedded key-value store** optimized for low-latency online feature serving (ML inference).
It is perfect for building recommendation engines and AI applications which need fast access to large feature data-sets in microseconds.

Key Features-
1. Data is fully persistent.
2. P99 read latency in 1-100 microseconds from client's point-of-view (depending on size of key/value and single or batch reads).
3. Scales w.r.t data size (partitions) and read-traffic (replicas).
4. Provides eventual read-after-write consistency.
5. Environment agnostic. Run on-prem or in any public cloud. "Database as a library" semantics.
6. Multi-language support with clients in Java (available), Python (coming in 2024) and Go (coming in 2024).
7. No need to provision/monitor/operate external database servers or incur encryption performance penalties.

What is an embedded database?

A database which is stored on the same physical machine as the user application/container. Due to this, there are no remote network calls to a "server" - providing orders of magnitude better read latency and throughput. 
This architecture is different from a traditional database solution (ex. DynamoDB or Firestore) - which involves calling single/multiple database servers over the network to retrieve data.

### Get Started with Java client
(Python and Go support incoming)





### Benchmarks
We measure read **latency** from a Java client's point of view, while accessing InlineKV.
Latency measurements - we track latency at various percentiles to measure delay in execution time that client's can expect to see.

#### Benchmarking environment - 
1. Single threaded client JVM instance using InlineKV's Java client (see /ikv-java-client)
2. Hardware - AWS r5.xlarge instance (4 vcpu, 32GB).

#### Parameters - 
1. We use byte arrays as the format for key and value. Key is ~ 10 bytes in size, Value is 350 bytes.
2. Single: i.e. return value for a single key
3. Batch: return values for the specified batch of keys
   
#### Results - 
Instance type - r5.xlarge (4 vcpu, 32GB)
Key: 10 bytes, Value: 50 bytes

| Type   | Parameters                         | Latency (microseconds)                             |
|--------|------------------------------------|----------------------------------------------------|
| SINGLE | num_samples:100,000                | avg: 1.07, p50: 1.00, p90: 1.00, p99: 3.00         |
| BATCH  | num_samples:10,000, batch_size:10  | avg: 5.40, p50: 5.00, p90: 7.00, p99: 10.00        |
|        | num_samples:10,000, batch_size:50  | avg: 18.44, p50: 18.00, p90: 23.00, p99: 33.00     |
|        | num_samples:10,000, batch_size:100 | avg: 32.37, p50: 31.00, p90: 39.00, p99: 51.00     |
|        | num_samples:10,000, batch_size:500 | avg: 242.69, p50: 228.00, p90: 278.00, p99: 347.00 |





