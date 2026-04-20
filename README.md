# Pocket FM

A state-of-the-art, industrial-scale data enrichment engine designed for **PocketFM**. This platform orchestrates a high-performance pipeline to discover, extract, and authorize deep metadata for thousands of titles across target genres (Romantasy, Paranormal Romance, Werewolves & Shifters).

## 🚀 Recent Milestone: 1,200 Titles Scaling Mission
Currently executing a massive expansion pass for the **Werewolves & Shifters** genre:
- **Completed**: 350+ titles processed with 100% fidelity.
- **Active Goal**: 1,200 titles (Automated in 20 sequential batches of 50).
- **Quality Standard**: Zero-loss "N/A" filtering for series metadata (Columns S-W).

## 🌟 Premium Features

- **Industrial Scaling Engine**: Automated multi-batch orchestration with state persistence and safety cool-downs to prevent IP flagging.
- **Multi-Tiered Discovery Intelligence**: 
    - **Amazon Tier**: Deep extraction of bestseller ranks, pricing, and series hierarchy.
    - **Goodreads Tier (Columns S-W)**: 5-level fallback search logic including Direct ASIN lookup, Series Extraction from titles, and Broad Discovery.
    - **Author Contact Tier**: Automated discovery of official websites, social media (FB/IG/Twitter), and professional representation (Agents).
- **Series Extraction Intelligence**: Automatically identifies series names from Amazon titles (e.g., matching parentheses) to priority-link Goodreads Series URLs.
- **Professional Excel Utility**: Generates high-fidelity `.xlsx` files with frozen headers, auto-fit row heights, and tiered primary-column mapping.

## 🛠️ Technical Stack

- **Backend**: Python 3.11, Playwright (Async), Pandas, OpenPyXL.
- **Advanced Logic**: Multi-tab extraction (Up to 15 concurrent tabs), Regex-based title normalization, and state-aware mission polling.
- **Frontend**: React 18, Vite, TypeScript, Tailwind CSS v4, Framer Motion.

---

## 📁 System Architecture

```text
PocketFM/
├── backend/
│   ├── keyword_scraper.py   # Main Industrial Mission Orchestrator
│   ├── scraper.py           # Multi-Tiered Intelligence (Amazon/Goodreads/Author)
│   ├── repair_goodreads.py  # Quality Assurance & Deep Metadata Repair
│   ├── excel_utility.py     # Professional Excel Sync & Formatting
│   └── keyword_state_shifters.json  # Live Mission Tracking
├── frontend/
│   ├── src/                 # Premium React UI for Mission Control
├── scraped_data_shifters.xlsx # Master Mission Output
└── task.md                  # Sequential Mission Roadmap
```

---

## 📊 Industrial Data Schema (33 Columns)

| Section | Key Data Points |
| :--- | :--- |
| **Amazon Meta** | Sub-Genre, Price Tier, Stars, Ratings, Rank, Print Length, Publisher. |
| **Goodreads (S-W)** | **Series URL**, Primary Book Count, Total Series Pages, Book 1 Rating, Book 1 Stats. |
| **Creative Content** | Loglines, One-Sentence hooks, Romantasy classification. |
| **Author Contact** | Email, Agent Email, Website, Social Media (Facebook, Instagram, Twitter). |

---

## ⚖️ Quality Standards
The platform enforces the **"Total Fidelity"** rule:
1. **Deduplication**: Automatic filtering of Amazon "Sponsored" results and repeating titles.
2. **Series Resolution**: Every book is cross-checked against the Goodreads database to resolve series links.
3. **Contact Verification**: Multi-source validation for author and agent emails.

---

> [!IMPORTANT]
> This platform is currently running in **Mission Mode**. For log visibility and real-time terminal feedback, monitor the `backend/` logs during the 1,200-title scaling pass.
