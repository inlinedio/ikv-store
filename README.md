# Inline I/O

Inline I/O provides scalable **embedded database** infrastructure, for latency sensitive applications. Inline I/O databases do not require any network calls (aka RPCs) and store the entire dataset in memory (with option to spill to disk), enabling **microsecond** level read latency.

While providing embedded read access, the platform provides all the features of a traditional Database-as-a-Service platform like - persistence and backup, horizontal scalability (replicas and shards), bulk loading and bootstrap. Inline I/O is a perfect choice for recommendation engines and AI products which need fast access to a lot of data.

Inline I/O is written in Rust, and provides clients in Java and Python.

## InlineKV
InlineKV is a distributed NoSQL **Key-Value store**.


