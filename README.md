# CivicSustainIQ: Fabric-to-Foundry Housing & Socio-Economic Analytics

CivicSustainIQ is an enterprise-grade AI and Data Engineering platform built to transform how regional authorities, urban planners, and environmental agencies address housing inefficiencies and carbon output.

By integrating a unified **Microsoft Fabric Lakehouse Medallion Architecture** with an advanced **Operational Data Foundry & Ontology Layer**, the platform seamlessly blends fragmented national datasets—Building Energy Ratings (BER), Central Statistics Office (CSO) geospatial boundaries, and Pobal Socio-Economic Deprivation Indices—into a living, actionable digital twin of the Irish housing ecosystem.

Video Demo : https://youtu.be/35ds2cHNtps
---

## 🏗️ Enterprise Architecture: The Medallion-to-Foundry Pipeline

The platform eliminates traditional data silos by orchestrating an automated pipeline that transitions raw national infrastructure data into a high-performance analytical engine, and finally into an operational foundry for front-line decision-making.

> **[Raw National Data]** ──> 🥉 **Bronze** ──> 🥈 **Silver** ──> 🥇 **Gold (Lakehouse)** ──> 🌐 **Foundry (Operational Ontology)**

### 🥉 Bronze Layer: Secure Ingestion

Acts as the central landing zone within Fabric OneLake. It ingests massive, unstructured national building energy data, high-resolution geospatial census shapefiles, and socio-demographic indicators without disrupting source operations.

### 🥈 Silver Layer: Enterprise Harmonization

Applies programmatic data-cleansing, type casting, and schema normalization. This layer standardizes disjointed geographic naming conventions across different government agencies, eliminates categorical anomalies, and isolates continuous metrics like carbon footprints and energy intensity.

### 🥇 Gold Layer: Relational Star Schema Model

Materializes a high-performance, direct relational database structure inside the `lh_sustainability_gold` Lakehouse. Instead of relying on rigid, abstract semantic layers, CivicSustainIQ connects a central energy assessment Fact table directly to surrounding dimensions via physical surrogate keys, enabling lightning-fast analytical queries.

---

## 🌐 The Operational Foundry: Moving from Analytics to Action

While the Gold layer answers *what* is happening, the **Foundry Layer** dictates *how to respond*. By mapping the Gold Lakehouse schemas into an interactive, object-oriented ontology, CivicSustainIQ transitions from a read-only reporting tool to a dynamic operational command center.

### Core Foundry Capabilities:

* **The Digital Twin Ontology:** Tabular data is transformed into real-world business objects. A row in a database becomes an interactive "Building Entity" that inherently knows its "Heating System," its "Deprivation Zone," and its "Carbon Target."
* **Simulated Intervention Scenarios:** Planners can adjust parameters—such as changing a region's primary heating fuel from Oil to Biomass—and instantly view the projected drop in CO2 emissions cascading through the ecosystem.
* **Write-Back & Workflow Management:** Users are not just viewing data; they are interacting with it. Planners can flag specific low-efficiency clusters, create actionable retrofitting cohorts, and track the status of grant allocations directly within the Foundry environment.
* **Socio-Economic Threat Modeling:** The Foundry continuously monitors the intersection of energy ratings and Pobal deprivation indices, automatically triggering alerts when vulnerable regions fall behind national decarbonization thresholds.

---

## 🤖 Prize-Winning Innovation: The Direct Lakehouse AI Agent

To democratize this complex ontology for non-technical urban planners and policy makers, CivicSustainIQ integrates an advanced, LLM-powered **Fabric Data Agent**.

### Key Capabilities & Technical Differentiation:

* **Zero Semantic Dependency:** Unlike standard implementations, this agent is trained directly on the physical Lakehouse relational topology and the Foundry ontology. It translates natural language questions directly into optimized, endpoint-compliant T-SQL expressions on the fly.
* **Dialect Optimization & Guardrails:** Fully aligned with strict SQL Endpoint constraints, utilizing native row-limiting strategies and zero-division math protections to ensure enterprise-grade reliability and sub-second response times.
* **Complex Matrix Aggregations:** Built to handle messy, categorical data distributions natively, allowing users to query qualitative patterns (like specific heat-pump configurations or legacy rating trends) across whole counties through a simple chat interface.

---

## 🎯 Strategic High-Value Use Cases

CivicSustainIQ empowers stakeholders to move from raw data to targeted, high-impact civic intervention:

1. **Targeted Retrofitting Campaigns:** Instantly maps clusters of low-efficiency, high-emission properties against specific fossil-fuel heating types to pinpoint neighborhoods primed for immediate heat-pump conversion grants.
2. **Socio-Economic Just Transition:** Cross-references housing energy poverty with Pobal Deprivation metrics, ensuring local authorities allocate sustainability funding to the most vulnerable communities first.
3. **Decarbonization Benchmarking:** Empowers urban planners to evaluate carbon emissions across distinct construction eras, proving the measurable impact of modern building regulations over legacy infrastructure.

---

## 🛠️ Agile CI/CD Framework

Built for rapid, scalable production deployment, CivicSustainIQ leverages native **Git Integration** backed by an automated CI/CD synchronization runner. Every notebook, orchestration pipeline, and Lakehouse schema definition is managed entirely as infrastructure-as-code (IaC), ensuring seamless collaboration, version stability, and deployment readiness for enterprise scaling.
