# Inline I/O

Inline I/O provides database infrastructure which enables extremely low latency online data access, for enterprise grade applications like recommendation engines and ML inference services.
Inline databases provide eventual read-after-write consistency and can scale to handle huge datasets with sharding/partitioning.

Indexes used for reads do not need any remote network calls and are stored in the same container/server as the hosting application - hence they can provide microsecond level latency (as compared to 10-100's of millisecond latency provided by cloud solutions). Inline I/O provides all the necessary components like pub-sub, backup, nearline/batch writers out of the box - which makes productionizing a local database achievable.

Inline I/O databses have clients in many popular languages like Java and Python to power your usecase. Get started now, the following versions are available for production use - 

## InlineKV
InlineKV is a distributed NoSQL **Key-Value store** with online readers, nearline/offline/batch writer components. InlineKV is ideal for usecases which need extremely low access latency, potentially make multiple (100-1000)'s of read calls per application request, and cannot afford to make outbound network calls.

### Quick Start
Read installation instructions.


