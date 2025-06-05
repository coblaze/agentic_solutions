# LLM Evaluator Pipeline

A fully autonomous, production-ready pipeline for evaluating AI-generated summaries against call transcripts. Runs daily at 12:01 AM without human intervention.

## Features

- **Fully Autonomous**: Runs daily at 12:01 AM CST, evaluating the previous day's data
- **Automatic Recovery**: Detects and recovers missed or failed batches without human intervention
- **Production Hardened**: Comprehensive error handling, retry logic, and health monitoring
- **Multi-Provider Email**: Supports SMTP, SendGrid, and AWS SES with automatic fallback
- **Detailed Reporting**: Excel reports with metrics and individual evaluation results
- **Smart Alerting**: Alerts only when accuracy falls below threshold or manual intervention needed
- **State Management**: Tracks all batch executions for complete audit trail

## Architecture
┌─────────────────┐ ┌──────────────┐ ┌─────────────────┐ │ Scheduler │────▶│ Pipeline │────▶│ Evaluation │ │ (12:01 AM) │ │ Orchestrator │ │ Service │ └─────────────────┘ └──────────────┘ └─────────────────┘ │ │ ▼ ▼ ┌──────────────┐ ┌─────────────────┐ │ MongoDB │ │ Google Vertex │ │ Database │ │ AI │ └──────────────┘ └─────────────────┘ │ ▼ ┌──────────────┐ ┌─────────────────┐ │ Report │────▶│ Email │ │ Service │ │ Service │ └──────────────┘ └─────────────────┘
Copy

## Installation

### Prerequisites

- Python 3.8+
- MongoDB 4.4+
- Google Cloud Project with Vertex AI enabled
- Email service credentials (SMTP/SendGrid/AWS SES)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourcompany/llm-evaluator.git
cd llm-evaluator
Create virtual environment:

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
Install dependencies:

pip install -r requirements.txt
Configure environment:

cp .env.example .env
# Edit .env with your configuration
Set up Google Cloud credentials:

export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
