# Kubernetriks (k10s)
Kubernetriks = k8s + matrix ("What is the Matrix? Control." (Morpheus, The Matrix, 1999))

Framework to simulate k8s cluster, workload scheduling and autoscaling based on DSLab framework for distributed systems simulations.

Run:
```
cargo test && cargo test -- --ignored
cargo run --release -- --config-file src/config.yaml
```
