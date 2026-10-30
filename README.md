# Cloud, AI & eBPF Engineering Tutorials (2026 Edition)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Kubernetes: v1.31+](https://img.shields.io/badge/Kubernetes-v1.31%2B-326CE5.svg)](https://kubernetes.io/)
[![eBPF: Cilium%20%7C%20Tetragon](https://img.shields.io/badge/eBPF-Cilium%20%7C%20Tetragon-yellow.svg)](https://cilium.io/)
[![GenAI: vLLM%20%7C%20LangGraph](https://img.shields.io/badge/GenAI-vLLM%20%7C%20LangGraph-green.svg)](https://github.com/vllm-project/vllm)
[![Observability: OpenTelemetry](https://img.shields.io/badge/Observability-OpenTelemetry%20%7C%20SigNoz-purple.svg)](https://signoz.io/)

A curated, production-tested portfolio of **8 in-depth, publication-ready technical tutorials** covering high-throughput LLM serving, multi-tenant GPU virtualization, kernel-level eBPF security, zero-broker lakehouse CDC pipelines, and zero-code distributed tracing.

Each tutorial is an end-to-end, reproducible blueprint featuring real-world infrastructure manifests (`Terraform`, `Helm`, `Kubernetes YAML`), complete application source code (`Python`, `Go`, `Bash`), and quantitative performance benchmarks.

---

## Portfolio Index & Technical Matrix

| # | Tutorial File | Target Ecosystem | Primary Architecture & Technologies |
| :-: | :--- | :--- | :--- |
| **01** | [`01-vultr-vke-deepseek-vllm-keda.md`](./01-vultr-vke-deepseek-vllm-keda.md) | **Vultr Cloud GPU & VKE** | NVIDIA A100/H100, vLLM (`TP=2`, PagedAttention), KEDA Queue-Depth Autoscaling, Prometheus DCGM |
| **02** | [`02-vultr-cilium-clustermesh-wireguard.md`](./02-vultr-cilium-clustermesh-wireguard.md) | **Vultr Multi-Region VKE** | Vultr VPC 2.0 Peering, Cilium ClusterMesh, In-Kernel WireGuard eBPF Encryption |
| **03** | [`03-airbyte-agentic-rag-langgraph.md`](./03-airbyte-agentic-rag-langgraph.md) | **Airbyte & PyAirbyte** | PyAirbyte (`airbyte-lib`), PostgreSQL `pgvector` HNSW, LangGraph Self-Correcting RAG State Machine |
| **04** | [`04-airbyte-cdc-motherduck-iceberg.md`](./04-airbyte-cdc-motherduck-iceberg.md) | **Airbyte Data Engineering** | PostgreSQL Logical Replication (`pgoutput`), PyAirbyte, MotherDuck / DuckDB, Apache Iceberg, Dagster |
| **05** | [`05-civo-k3s-cilium-tetragon.md`](./05-civo-k3s-cilium-tetragon.md) | **Civo Cloud K3s** | Civo K3s (`--cni=none`), Cilium eBPF kube-proxy Replacement, L7 NetworkPolicies, Tetragon SIGKILL Enforcement |
| **06** | [`06-civo-vcluster-gpu-timeslicing.md`](./06-civo-vcluster-gpu-timeslicing.md) | **Civo Cloud GPU** | Civo NVIDIA A100/L40S, Loft Labs vCluster, NVIDIA GPU Time-Slicing (`4x` Density), Cilium Isolation |
| **07** | [`07-signoz-ebpf-zero-code-tracing.md`](./07-signoz-ebpf-zero-code-tracing.md) | **SigNoz & OpenTelemetry** | OpenTelemetry eBPF Instrumentation (`OBI` / `Beyla`), Go & Python Microservices, SigNoz ClickHouse APM |
| **08** | [`08-signoz-langgraph-llm-observability.md`](./08-signoz-langgraph-llm-observability.md) | **SigNoz GenAI APM** | Multi-Agent LangGraph, OpenTelemetry GenAI Semantic Conventions, Token Burn & Latency Alerting |

---

## Author
**Duong Y Nhi** ([@duongynhi000005-oss](https://github.com/duongynhi000005-oss))
