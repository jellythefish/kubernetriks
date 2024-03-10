# kubernetriks
kubernetriks = k8s + matrix ("What is the Matrix? Control." (Morpheus, The Matrix, 1999))

Framework to simulate k8s cluster, workload scheduling and autoscaling based on DSLab framework for distributed systems simulations.

Run prototype:
```
cargo test
RUST_LOG=trace cargo run -- --config-file src/config.yaml --trace-file src/data/generic_trace_example.yaml
```
