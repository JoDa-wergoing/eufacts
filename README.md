# EUFacts — Making EU data understandable

**EUFacts** is an open-source initiative that collects, standardizes and visualizes key European Union statistics — such as government debt, budget contributions and EU-funded expenditures — to make European data transparent and accessible for everyone.

> _“Europe in numbers — clear, verifiable, open.”_

---

## 🎯 Purpose
European data is publicly available, but often scattered across many portals (Eurostat, EU Budget, Cohesion Data, FTS, etc.) and hard to interpret without technical skills.  
**EUFacts** aims to bridge that gap by:
- Aggregating data from verified EU sources.  
- Normalizing it into consistent, documented datasets (JSON/CSV).  
- Publishing accessible visualizations with clear explanations and context.  
- Offering transparent versioning so figures can be verified and reproduced.

---

## 📊 Data sources (initial phase)
| Source | Dataset | Description |
|---------|----------|-------------|
| [Eurostat](https://ec.europa.eu/eurostat/) | TEINA230 | General government gross debt (% of GDP) |
| [European Commission](https://commission.europa.eu/) | EU Budget — Spending & Revenue | Contributions and expenditures by Member State |
| [Financial Transparency System (FTS)](https://ec.europa.eu/budget/financial-transparency-system/analysis.html) | Grants & contracts | Beneficiaries of EU-funded projects |
| [Cohesion Data Portal](https://cohesiondata.ec.europa.eu/) | Regional programmes | EU Structural & Investment Fund projects |

---

## 🧱 Repository structure
/etl → Python scripts for data ingestion & normalization
/data → JSON/CSV snapshots (latest + historical)
/apps/frontend → Next.js website (static + serverless functions)
/docs → Methodology, dataset notes, changelog
.github/workflows → CI/CD pipelines for ETL & deployment

---

## 🚀 Development status
| Stage | Description |
|--------|-------------|
| MVP (v0.1) | Eurostat debt dataset + methodology page |
| Beta (v0.2) | EU Budget (contributions/receipts) + i18n support |
| Public (v1.0) | Full deployment with monitoring and open dataset portal |

Follow progress on the [**Project Board → EUFacts-MVP**](../../projects).

---

## 🧰 Tech stack
- **Frontend:** Next.js + TypeScript + Recharts + MapLibre  
- **Data/ETL:** Python (pandas, pyarrow)  
- **Storage:** Cloudflare R2 / S3  
- **Automation:** GitHub Actions (daily ETL, deploy to Vercel)  
- **License:** MIT (code) + CC BY 4.0 (data, where applicable)

---

## 📜 Methodology & transparency
All datasets include:
- A `manifest.json` with source URLs, version date, and SHA-256 checksum.  
- Notes on scope, known limitations, and update frequency.  
- Clear distinction between **original data**, **derived metrics**, and **visualized indicators**.  

Read more in [`/docs/methodology.md`](./docs/methodology.md).

---

## 💬 Contributing
Contributions are welcome — whether data adapters, UI improvements, or translation support.  
Please:
1. Fork the repository  
2. Create a feature branch  
3. Submit a pull request  

Open discussions or improvement ideas via [Issues](../../issues).

---

## 📅 Update schedule
| Dataset | Frequency | Last updated |
|----------|------------|---------------|
| Eurostat Debt (TEINA230) | Weekly | — |
| EU Budget (Spending & Revenue) | Monthly | — |
| FTS Beneficiaries | Annual (rolling) | — |

---

## 📄 License
- **Code:** [MIT License](./LICENSE)  
- **Data:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (requires attribution to original EU sources)  
- **Attribution example:** “Contains data from Eurostat, © European Union, reused under the CC BY 4.0 license.”

---

## 🌍 Links
- Website:*(coming soon)*  
- Project Board: [EUFacts-MVP](../../projects)  
- Data Portal (API): *(in development)*  
- Contact: `info@wergoing.com`

---

> © 2025 EUFacts — open data for an informed Europe.
