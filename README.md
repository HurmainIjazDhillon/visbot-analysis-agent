# VisBot Analysis Agent

An intelligent IoT asset analysis agent that translates natural language questions into SQL queries against an OpenRemote PostgreSQL database and returns structured, human-readable reports — both through a chat UI and a machine-to-machine API.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend API | FastAPI + Uvicorn |
| AI / LLM | LangChain + Groq (LLM inference) |
| NLP → SQL | Custom NL-to-SQL pipeline with SQL guard |
| Deterministic Analysis | Python-based asset analyzers |
| Scheduling | LangChain agent with scheduling logic |
| Database | PostgreSQL (OpenRemote schema) |
| Frontend | Next.js 14 (App Router) + TypeScript |
| Styling | Tailwind CSS |

---

## What this agent does

- Accepts a natural language question (e.g. *"Analyze Cold Room 1 for the last 2 hours"*)
- Resolves the asset name and time window from the message
- Selects the correct analysis plan based on per-asset business rules
- Generates and executes a safe SQL query against the OpenRemote database
- Runs deterministic Python analysis (temperature stability, production counters, tank level, smoke alarm checks, etc.)
- Returns a structured report in chat and via API for a Super Agent to consume

---

## Project Structure

```
Analysis Agent/
├── backend/
│   ├── app/
│   │   ├── api/routes/          # HTTP route handlers (assets, chat, analysis)
│   │   ├── core/                # App settings (pydantic-settings)
│   │   ├── models/              # Request / response Pydantic schemas
│   │   ├── prompts/             # LLM prompt templates
│   │   ├── services/
│   │   │   ├── analysis_agent.py            # Main LangChain agent orchestrator
│   │   │   ├── scheduling_agent.py          # Scheduling + routing agent
│   │   │   ├── deterministic_analysis.py    # Rule-based analyzers per asset type
│   │   │   ├── nl_to_sql.py                 # NLP-to-SQL query generator
│   │   │   ├── sql_guard.py                 # SQL safety validator
│   │   │   ├── llm_service.py               # Groq LLM wrapper
│   │   │   ├── asset_analysis_instructions.py  # Per-asset analysis rules
│   │   │   ├── asset_catalog.py             # Asset registry loader
│   │   │   ├── live_asset_registry.py       # Live asset state manager
│   │   │   ├── report_builder.py            # HTML report generator
│   │   │   ├── trend_chart_service.py       # Trend chart data builder
│   │   │   └── data_repository.py           # DB query executor
│   │   └── templates/           # Jinja2 HTML report templates
│   └── data/
│       └── asset_catalog.yaml   # Asset inventory and metadata
├── frontend/
│   ├── app/                     # Next.js App Router pages
│   ├── components/
│   │   └── chat-shell.tsx       # Main chat UI component
│   └── lib/                     # API client + TypeScript types
├── .env.example                 # Environment variable template
├── requirements.txt             # Python dependencies
└── README.md
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `GROQ_API_KEY` | Your Groq API key — get one at [console.groq.com](https://console.groq.com) |
| `GROQ_MODEL` | Groq model name (e.g. `llama-3.3-70b-versatile`) |
| `DATABASE_URL` | PostgreSQL connection string (e.g. `postgresql://user:pass@host:5432/openremote`) |
| `LOCAL_TIMEZONE` | Your local timezone (e.g. `Asia/Karachi`) |
| `LOCAL_TIME_OFFSET_HOURS` | UTC offset in hours (e.g. `5`) |
| `OPENREMOTE_SCHEMA` | PostgreSQL schema name (default: `openremote`) |
| `NEXT_PUBLIC_API_BASE_URL` | Backend URL visible to the browser (default: `http://localhost:8000`) |

---

## Local Setup

### Prerequisites

- Python 3.11+
- Node.js 18+
- A running PostgreSQL database with the OpenRemote schema

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/visbot-analysis-agent.git
cd visbot-analysis-agent
```

### 2. Set up the backend

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and fill in env vars
cp .env.example .env

# Start the backend
cd backend
uvicorn app.main:app --reload
```

Backend will be available at `http://localhost:8000`

- Swagger docs: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`

### 3. Set up the frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend will be available at `http://localhost:3000`

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/api/v1/assets` | List all registered assets |
| `GET` | `/api/v1/assets/{asset_id}` | Get a single asset's metadata |
| `POST` | `/api/v1/chat/message` | Send a chat message, get an analysis report |
| `POST` | `/api/v1/analysis/run` | Run a direct analysis (M2M / Super Agent) |

### Example: Chat request

```bash
curl -X POST http://localhost:8000/api/v1/chat/message \
  -H "Content-Type: application/json" \
  -d '{"message": "Analyze Cold Room 1 for the last 2 hours"}'
```

### Example: Direct analysis request

```bash
curl -X POST http://localhost:8000/api/v1/analysis/run \
  -H "Content-Type: application/json" \
  -d '{
    "asset_id": "coldroom_1",
    "question": "Is the temperature stable in Cold Room 1 over the last hour?"
  }'
```

---

## Adding a New Asset

1. **Register the asset** in `backend/data/asset_catalog.yaml`:

```yaml
- id: my_asset_1
  name: My New Asset
  type: sensor
  table: openremote.asset_datapoint
  supported_analysis: [trend, anomaly]
```

2. **Add analysis rules** in `backend/app/services/asset_analysis_instructions.py` — define what metrics matter and how to interpret them.

3. **Add a deterministic analyzer** in `backend/app/services/deterministic_analysis.py` if you want Python-based (non-LLM) calculations for this asset type.

---

## Supported Asset Types

| Asset Type | Metrics Analyzed |
|---|---|
| Cold Room | Temperature stability, humidity, out-of-range alerts |
| Filling Machine | Production counters, shift output, downtime |
| Oil / Water Tank | Current level (ft), fill percentage, level trend |
| Smoke Alarm | Alarm state, smoke level, battery, temperature |

---

## Architecture Overview

```
User message
     │
     ▼
Next.js Frontend  ──POST /api/v1/chat/message──►  FastAPI Backend
                                                        │
                                          ┌─────────────▼──────────────┐
                                          │      Analysis Agent         │
                                          │  (LangChain orchestrator)   │
                                          └─────────────┬──────────────┘
                                                        │
                              ┌─────────────────────────┼──────────────────────┐
                              ▼                         ▼                      ▼
                     Asset Catalog              NL-to-SQL Engine        Asset Analysis
                     (YAML registry)            + SQL Guard             Instructions
                              │                         │
                              └──────────► PostgreSQL (OpenRemote) ◄────────────┘
                                                        │
                                          ┌─────────────▼──────────────┐
                                          │  Deterministic Analyzer     │
                                          │  (Python rule-based logic)  │
                                          └─────────────┬──────────────┘
                                                        │
                                          ┌─────────────▼──────────────┐
                                          │      Report Builder          │
                                          │  (structured JSON + HTML)   │
                                          └─────────────┬──────────────┘
                                                        │
                                                        ▼
                                              Response to chat UI
                                          or Super Agent via M2M API
```

---

## License

Private / Internal use only.
