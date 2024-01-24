# IKV | Inlined.io Key Value Store

IKV is a **fully-managed [embedded database](https://en.wikipedia.org/wiki/Embedded_database)**, optimized for online feature serving for ML inference. It is ideal for building large scale distributed systems (ex. recommendation engines or information retrieval tasks) - which need low latency access to key-value data. IKV-Store is -

 1. **Blazing Fast**: With no network overhead, IKV offers P99 read latency of **1-100 microseconds** from client’s point-of-view (depending on key/value size and single/batch operations). This is orders of magnitude better (10x) than existing solutions like Redis.
 2. **Built for the Enterprise**: Extremely low operational overhead; pay-as-you-go with no provisioned hardware costs; Environment agnostic - use in the public cloud or on-prem.
 3. **Horizontally Scalable**: Handles large datasets (with partitions) and high read/write traffic (with replication). Built for streaming and batch ingestion of data.
 4. **Fully Persistent with Backup Data-Lake**: Write once and read forever. Read more about IKV's unique architecture which makes this possible for an embedded database.

## Quick Links
 - Getting Started
	 - Java
	 - Python (upcoming - July 2024)
	 - Go (upcoming - July 2024)
 - Benchmarks
 - APIs
 - Architecture
 - FAQ
 - Contact inlined.io

## Getting Started with Java
In this section we go over some code samples about how to use IKV's client library in your Java project.

#### Installation
`ikv-java-client` is hosted with Github Packages, add dependency to your Gradle/Maven project.
Make sure to use the latest version from [package list](https://github.com/inlinedio?tab=packages&repo_name=ikv-store).

```
repositories {
  maven {
    name = "GitHubPackages"  
    url = uri("https://maven.pkg.github.com/inlinedio/ikv-store")  
    credentials {
      // You can use the following public github tokens, or inject your own personal tokens.
      username = "inlinedio"  
      password = "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"  
    }
  }

 // ... other project repositories (ex. mavenCentral()) ..
}

dependencies {
  implementation group: 'io.inlined', name: 'ikv-java-client', version: '0.0.3'

  // .. other project dependencies ..
}
``` 
```
<dependency>
  <groupId>io.inlined</groupId>
  <artifactId>ikv-java-client</artifactId>
  <version>0.0.3</version>  
</dependency>
```




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





