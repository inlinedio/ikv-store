---
slug: /
sidebar_position: 1
---

# IKV | Inlined Key-Value Store
IKV is a high-performance **fully-managed, embedded key-value** store for powering ML inference. It's unique design tradeoffs makes it perfect for accessing large key-value datasets with very low latency in a production setting. This website provides detailed documentation for using IKV in your application.
 - [Onboarding](./onboarding.md)
 - [Data Modeling](./data-modeling.md)
 - [Clients (Language Specific SDKs)](./category/clients)


## About IKV
#### Embedded & Blazing Fast
IKV is an eventually-consistent, partitioned **[embedded database](https://en.wikipedia.org/wiki/Embedded_database)** on top of a backend data layer. IKV can serve read requests without making any network calls, and provides **single-digit microsecond** P99 read latency from a client’s point-of-view. This is **100x faster than existing solutions like Redis**. Read our benchmarks [here](https://docs.google.com/document/d/1aDsS0V-AybpvXEwblBlahGLpKH5NmUmi6mTWGsbABGk/edit#heading=h.ey4ngxmm384e).

IKV is heavily optimized for read performance (latency/throughput):
 - In-memory with option to spill to local disk.
 - Designed for point-lookups ("hashtable/dictionary API").
 - No cold start problem or RPCs during cache misses.
 - No Garbage Collection. IKV is written in Rust, with thin clients in languages like Java, Go & Python.

#### Fully Managed
IKV is more than just a library - it handles all data management aspects for you. It provides "write once, read forever" semantics, i.e. data is fully persistent (stored in IKV cloud). It replicates data across your fleet of application containers (globally). IKV scales horizontally for large datasets (with partitions) and high read/write traffic (with replication). This makes IKV unique as compared to other "library-only" embedded databases (ex. LevelDB, RocksDB or LMDB).

## Usecases for IKV
Typical usecases include recommendation-engines, ML inference (feature stores) and information-retrieval related tasks; which make several database calls per application request. IKV is for you if you -
 - Do not need strong read-after-write consistency for your data.
 - Have tight latency requirements for large key-value data sets