# Pocket FM

A high-performance, full-stack application designed for PocketFM to scrape and extract structured data from Amazon Bestseller lists. It features deep-link detail extraction, high-concurrency tab orchestration, and professional Excel data formatting.

## 🌟 Key Features

- **High-Concurrency Scraping**: Orchestrates up to 15 concurrent tabs in a single browser instance to minimize extraction time.
- **Deep Extraction (Part 2)**: Automatically visits every individual product page to extract Descriptions, Publishers, and Publication Dates.
- **Professional Excel Export**: Generates auto-sized, text-wrapped Microsoft Excel (`.xlsx`) files using Pandas and OpenPyXL.
- **Data Cleaning Engine**: Standardizes ratings, review counts, and normalizes unicode text (NFKD) to remove zero-width characters and stray punctuation.
- **AI-Driven UI**: A modern, glassmorphic React interface featuring the **Premium Prompt Box**, real-time terminal logs, and TypeScript type safety.

## 🛠️ Technical Stack

- **Backend**: Python 3.11, Flask, Playwright (Async), Pandas, OpenPyXL.
- **Frontend**: React 18, Vite, **TypeScript**, **Tailwind CSS v4**, Framer Motion, Radix UI.
- **Orchestration**: `concurrently` (runs Flask and Vite with one command).

---

## 📁 Project Structure

```text
PocketFM/
├── backend/
│   ├── app.py              # Flask API & Async Orchestrator
│   ├── scraper.py          # Core Playwright Scraping Logic
│   ├── excel_utility.py    # Professional Excel Formatting
│   └── venv/               # Python Virtual Environment
├── frontend/
│   ├── src/
│   │   ├── components/ui/  # Premium AI Prompt Box & shadcn components
│   │   ├── lib/utils.ts    # Tailwind Merging Utility
│   │   ├── App.tsx         # Modernized React UI (TSX)
│   │   └── index.css       # Tailwind v4 Design System
│   ├── tailwind.config.js  # Theme Configuration
│   ├── tsconfig.json       # TypeScript Configuration
│   └── package.json        # Frontend Dependencies
├── README.md               # Main Documentation
└── package.json            # Unified Scripts & Root Config
```

---

## 🚀 Getting Started

### 1. Prerequisites
- **Node.js**: For the React frontend.
- **Python 3.11+**: For the Flask backend scraper.

### 2. Installation
Run the following in the root directory:
```bash
# Install root orchestration tools
npm install

# Install frontend dependencies
cd frontend && npm install && cd ..

# Setup backend environment is already handled via venv
```

### 3. Running the Application
The entire stack (Frontend + Backend) can be launched with a single command:
```bash
npm run dev
```
- **Frontend**: [http://localhost:5173](http://localhost:5173)
- **Backend API**: [http://localhost:5000](http://localhost:5000)

---

## 📊 Data Points Extracted

| Field | Source | Extraction Logic |
| :--- | :--- | :--- |
| **Rank** | Bestseller List | Numeric index from ranking badge |
| **Book Title** | Bestseller List | Primary product name |
| **Author Name** | Bestseller List | Normalized author field |
| **Price** | Bestseller List | Current market price |
| **Rating** | Bestseller List | Float conversion of star count |
| **Description** | Product Page | Full text normalization (Part 2) |
| **Publisher** | Product Page | Extracted from Detail Bullets (Part 2) |
| **Publication Date** | Product Page | Standardized date extraction (Part 2) |

---

## ⚖️ Assignment Compliance
This project fulfills the full requirements of the PocketFM Scraper assignment:
- [x] **Part 1**: Extract list-level data (Rank, Title, Author, etc.).
- [x] **Part 2**: Deep extraction from individual product pages.
- [x] **Part 3**: Clean and structure data (Clean URLs, handle missing values).
- [x] **Export**: Automatic download of a professionally formatted Excel sheet.

---

> [!IMPORTANT]
> This application uses **Headed Mode** (browser windows will open) by default to satisfy assignment visibility requirements. To run in the background, set `headless=True` in `backend/app.py`.
