---
slug: /
sidebar_position: 1
---

# IKV | Inlined Key-Value Store
IKV is a key-value store primarly designed for storing ML features. It is persistent, eventually-consistent and most importantly an **embedded database**. IKV is written in Rust, and provides SDKs (client libraries) in Python, Go and Java. This website provides detailed documentation for using IKV in your application.
 - [Onboarding](./onboarding.md)
 - [Data Modeling](./data-modeling.md)
 - [Clients (Language Specific SDKs)](./category/clients)


## About IKV
#### Cost-effective caching at sub-millisecond speed
Most key-value stores require you to allocate a dedicated database cluster which is costly (hardware + operations). IKV allows you to augment your existing application cluster with additional memory or Flash/SSD to store your data, without sacrificing on speed.
 - Sub-millisecond access latency when used with local (attached) Flash/SSD storage, which is still 2x faster than a remote all-in-memory Redis cluster
 - Automatic data-tiering and zero-copy disk reads.

#### Embedded & Blazing Fast
IKV can serve read requests without making any network calls, and provides **single-digit microsecond** P99 read latency from a client’s point-of-view. This is **100x faster than existing solutions like Redis**. Read our benchmarks [here](https://docs.google.com/document/d/1aDsS0V-AybpvXEwblBlahGLpKH5NmUmi6mTWGsbABGk).

IKV is heavily optimized for read performance (latency and throughput):
 - Zero network latency for reads (even on a cold start). No overhead of network hops, encryption and minimal ser-deserialization.
 - Frequently accessed data occupies local memory.
 - No need for index compaction (like LSM tree based stores), hence there is no resource saturation. 
 - No Garbage Collection. IKV is written in Rust with client SDKs in Python, Go and Java.

#### Fully-managed, more than a library
Traditional embedded databases (like RocksDB or LevelDB) are just libraries, IKV does way more:
 - Data is fully persistent. Write once and read forever, without the need to perform any explicit backups or snapshots.
 - Geo-replicated. IKV's underlying data-plane (IKV Cloud or self-hosted) replicates your data globally across your application clusters.
 - Horizontal scaling - IKV scales horizontally for large datasets (with partitions) and high read/write traffic (with replication).

#### Usecases for IKV
IKV's primary usecase is for storing ML features for online retrieval - as part of ML inference/scoring for applications like recommendation engines, search, ecommerce, etc.
It can also be used for fast general-purpose storage/caching - anything with tight latency requirements and doesn't need read-after-write consistency.