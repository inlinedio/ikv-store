# Inline I/O
Inline I/O is an **embedded database platform**, which enabled data reads in a few microseconds over large datasets.
The platform does not require any network calls (aka RPCs) and store the entire dataset in memory (with option to spill to disk).

While providing an embedded database, the following functionality is provided (critical for enterprise use) - 
1. Horizontal scalability (replicas and sharding) to scale for traffic and data size.
2. Data persistence, backup and geo-replication (write once, read everywhere semantics)
3. Bulk loading of data and bootstrap of new instances
 
Inline I/O provides the foundation of data intensive applications like recommendation engines, which retrieve and rank large data sets and have strict latency requirements.

## InlineKV
InlineKV is a NoSQL key-value store, with embedded read access.
Written in Rust, client libraries are available in Java and Python.

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
| Type   | Parameters                      | Latency (micro-seconds)                                       |
|--------|---------------------------------|---------------------------------------------------------------|
| SINGLE | num_samples:1000                | avg: 1.33, p50: 1.00, p90: 3.00, p99: 4.00, pMax: 9.00        |
| BATCH  | num_samples:1000,batch_size:10  | avg: 7.65, p50: 7.00, p90: 10.00, p99: 27.00, pMax: 136.00    |
|        | num_samples:1000,batch_size:50  | avg: 53.26, p50: 45.00, p90: 62.00, p99: 79.00, pMax: 5234.00 |
|        | num_samples:1000,batch_size:100 | avg: 76.05, p50: 73.00, p90: 89.00, p99: 108.00, pMax: 130.00 |





