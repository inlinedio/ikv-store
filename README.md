Inline I/O provides databases for online serving that live in the same container as the hosting application - and provide extremely low access latency by doing so.

# InlineKV
InlineKV is a distributed **Key-Value store** with online readers, nearline/offline/batch writer components. InlineKV is ideal for usecases which need extremely low access latency, potentially make multiple (100-1000)'s of read calls per application request, and cannot afford to make outbound network calls in general. 

Client libraries are available in - Java, Python and Rust (more coming soon).

