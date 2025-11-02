# FundLens API

FastAPI layer for browsing and searching FundLens campaign finance data.

## Features

- **Browse by hierarchy**: State → Race/Office → Candidate → Contributor → Contribution
- **Search**: Full-text search across candidates, contributors, and committees
- **Statistics**: Aggregated stats for candidates, contributors, and committees
- **Filtering**: Rich filtering options for contributions (date ranges, amounts, types)
- **Pagination**: Consistent pagination across all list endpoints
- **Type-safe**: Full Pydantic validation and OpenAPI documentation

## Project Structure

```
fund-lens-api/
├── fund_lens_api/
│   ├── main.py              # FastAPI app entry point
│   ├── config.py            # Pydantic settings
│   ├── dependencies.py      # DB session injection
│   ├── schemas/             # Pydantic response models
│   │   ├── common.py        # Pagination, filters
│   │   ├── candidate.py
│   │   ├── contributor.py
│   │   ├── committee.py
│   │   └── contribution.py
│   ├── services/            # Business logic
│   │   ├── candidate.py
│   │   ├── contributor.py
│   │   ├── committee.py
│   │   └── contribution.py
│   └── routers/             # API endpoints
│       ├── candidate.py
│       ├── contributor.py
│       ├── committee.py
│       └── contribution.py
├── pyproject.toml
└── README.md
```

## Installation

### Prerequisites

- Python 3.11+
- Poetry
- PostgreSQL database with FundLens gold layer data

### Setup

1. **Install dependencies**:
   ```bash
   poetry install
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your database URL
   ```

3. **Run the server**:
   ```bash
   poetry run dev
   # Or manually:
   poetry run uvicorn fund_lens_api.main:app --reload
   ```

4. **Access the API**:
   - API: http://localhost:8000
   - Interactive docs: http://localhost:8000/docs
   - OpenAPI spec: http://localhost:8000/openapi.json

## API Endpoints

### Candidates

- `GET /candidates` - List candidates with pagination and filtering
- `GET /candidates/search?q={query}` - Search candidates by name
- `GET /candidates/states` - Get list of states with candidates
- `GET /candidates/by-state/{state}` - Get candidates by state
- `GET /candidates/{id}` - Get candidate details
- `GET /candidates/{id}/stats` - Get candidate statistics

### Contributors

- `GET /contributors` - List contributors with pagination and filtering
- `GET /contributors/search?q={query}` - Search contributors by name
- `GET /contributors/top` - Get top contributors by amount
- `GET /contributors/{id}` - Get contributor details
- `GET /contributors/{id}/stats` - Get contributor statistics

### Committees

- `GET /committees` - List committees with pagination and filtering
- `GET /committees/search?q={query}` - Search committees by name
- `GET /committees/by-candidate/{id}` - Get committees by candidate
- `GET /committees/by-state/{state}` - Get committees by state
- `GET /committees/{id}` - Get committee details
- `GET /committees/{id}/stats` - Get committee statistics

### Contributions

- `GET /contributions` - List contributions with pagination and filtering
- `GET /contributions/summary` - Get aggregated statistics
- `GET /contributions/by-contributor/{id}` - Get contributions by contributor
- `GET /contributions/by-committee/{id}` - Get contributions by committee
- `GET /contributions/by-candidate/{id}` - Get contributions by candidate
- `GET /contributions/{id}` - Get contribution details
- `GET /contributions/{id}/full` - Get contribution with all related entities

## Usage Examples

### Browse candidates in Maryland

```bash
# Get all MD candidates
curl "http://localhost:8000/candidates/by-state/MD"

# Get candidate details
curl "http://localhost:8000/candidates/1"

# Get candidate fundraising stats
curl "http://localhost:8000/candidates/1/stats"
```

### Search contributors

```bash
# Search by name
curl "http://localhost:8000/contributors/search?q=Smith"

# Filter by employer
curl "http://localhost:8000/contributors?employer=Google&page=1&page_size=50"

# Get top contributors
curl "http://localhost:8000/contributors/top?limit=10"
```

### Filter contributions

```bash
# Get recent contributions
curl "http://localhost:8000/contributions?start_date=2024-01-01&page=1"

# Filter by amount range
curl "http://localhost:8000/contributions?min_amount=1000&max_amount=5000"

# Get contribution summary
curl "http://localhost:8000/contributions/summary?election_year=2026"
```

### Pagination

All list endpoints support pagination:

```bash
curl "http://localhost:8000/candidates?page=2&page_size=100"
```

Response includes pagination metadata:
```json
{
  "items": [...],
  "meta": {
    "page": 2,
    "page_size": 100,
    "total_items": 543,
    "total_pages": 6,
    "has_next": true,
    "has_prev": true
  }
}
```

## Development

### Run tests

```bash
poetry run pytest
```

### Code quality

```bash
# Format code
poetry run ruff format .

# Lint
poetry run ruff check .

# Type check
poetry run mypy fund_lens_api/
```

### Pre-commit hooks

```bash
poetry run pre-commit install
poetry run pre-commit run --all-files
```

## Configuration

Environment variables (see `.env.example`):

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `APP_NAME` | API name | "FundLens API" |
| `DEBUG` | Debug mode | false |
| `CORS_ORIGINS` | Allowed CORS origins | ["http://localhost:3000"] |
| `DEFAULT_PAGE_SIZE` | Default pagination size | 50 |
| `MAX_PAGE_SIZE` | Maximum pagination size | 1000 |

## Dependencies

- **FastAPI**: Web framework
- **SQLAlchemy 2.0**: ORM for database access
- **Pydantic v2**: Data validation and settings
- **fund-lens-models**: Shared SQLAlchemy models (from git)
- **uvicorn**: ASGI server

## License

MIT License - See LICENSE file for details
