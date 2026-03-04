# Project Argus: Distributed AML & High-Throughput Risk Engine

This document serves as the master blueprint for **Project Argus**. It outlines the distributed architecture, hardware allocation, and system flow to enable Copilot to assist in building a cloud-scale Anti-Money Laundering (AML) system on local heterogeneous hardware.

---

## 1. System Vision

**Argus** is a real-time, event-driven financial risk platform. It simulates a global banking network, ingesting thousands of transactions per second, performing in-memory graph analysis to detect money laundering cycles, and utilizing a distributed AI inference mesh (AMD, Intel, and NVIDIA hardware) to generate investigative reports.

---

## 2. Hardware & Service Mapping

| Component | Hardware | Software Stack | Role |
| --- | --- | --- | --- |
| **Control Plane** | Mac Pro (Xeon) | K3s (Kubernetes) | Orchestration & Master Node |
| **Ingestion** | Mac Pro (Xeon) | Golang | Goal of 100k TPS Transaction Generator |
| **Messaging** | Mac Pro (Xeon) | Redpanda (Kafka) | High-speed Event Bus |
| **Vault (Graph)** | Mac Pro (384GB) | Memgraph | In-memory Transaction Graph |
| **Auditor** | Mac Pro (W6800X Duo) | PyTorch (MPS) | Batch Embedding & Semantic Audit |
| **Investigator** | Proxmox (1660 Ti) | Ollama / Llama-3 | Agentic SAR (Investigative) Reporting |
| **Dashboard** | MacBook Air (M2) | Next.js / Three.js | Real-time Global Risk Visualization |

---

## 3. High-Level Architecture Flow

1. **Generate:** The Go-based `ingestor` pumps synthetic transactions into a local `Redpanda` topic.
2. **Stream:** `Memgraph` consumes the stream, mapping accounts as nodes and transactions as edges.
3. **Detect:** A Cypher-query worker identifies "Cyclic Clusters" (Money moving A -> B -> C -> A) in RAM.
4. **Audit:** High-risk transactions are batched and sent to the Mac Pro's **W6800X Duo** for semantic analysis (NLP).
5. **Investigate:** Flagged entities trigger an RPC call to the **Proxmox Agent** (NVIDIA), which uses an LLM to write a formal Suspicious Activity Report (SAR).
6. **Visualize:** All metrics (TPS, RAM usage, TFLOPS, Fraud Alerts) are pushed to the **React Dashboard**.

---

## 4. Repository Structure

```text
Argus/
├── infrastructure/           # Kubernetes & Docker Configs
│   ├── k3s/                  # K3s manifests (deployment.yaml, service.yaml)
│   ├── redpanda/             # Kafka/Redpanda config
│   └── monitoring/           # Prometheus & Grafana dashboards
├── services/
│   ├── ingestor-go/          # High-speed Transaction Generator (Golang)
│   │   ├── main.go
│   │   └── generator/
│   ├── vault-graph/          # Memgraph schemas and Cypher queries
│   │   └── migrations/
│   ├── auditor-mps/          # Mac Pro GPU Worker (Python/MPS)
│   │   ├── worker.py         # Batch processing logic
│   │   └── model/            # FinBERT integration
│   └── investigator-cuda/    # Proxmox Agentic Worker (Python/CUDA)
│       ├── agent.py          # Llama-3 / LangChain logic
│       └── prompts/          # SAR generation templates
├── dashboard-ui/             # Next.js + Three.js Frontend
│   ├── components/           # 3D Map & Real-time Charts
│   └── hooks/                # WebSocket listeners for Kafka events
├── shared/                   # Protobuf / Avro schemas for cross-service communication
└── scripts/                  # Stress-test and setup automation

```

---

## 5. Technical Constraints for Copilot

* **Backend:** Prioritize **Golang** for high-concurrency ingestion and **Python 3.12** for AI services.
* **GPU Backend:** Mac Pro services must use `torch.device("mps")`. Proxmox services must use `torch.device("cuda")`.
* **Security:** Use **Safetensors** for all model loading to satisfy CVE-2025-32434 requirements.
* **Efficiency:** Use **Batch Processing** for all inference (Batch Size: 128+) to overcome PCIe 3.0 latency on the Mac Pro.
* **Messaging:** Implement asynchronous communication using **Kafka (Redpanda)** to prevent the "Scout" from blocking the "Auditor."

---

## 6. Key Performance Indicators (KPIs)

* **Ingestion:** > 100,000 Transactions Per Second.
* **Graph Latency:** < 50ms for cycle detection across 1 million nodes.
* **Inference Speed:** < 5ms per document average (Batch mode).
* **RAM Footprint:** Utilize > 250GB of the 384GB Xeon RAM for the in-memory graph.

---
