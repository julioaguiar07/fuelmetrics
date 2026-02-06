web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
release: python -c "from app.models.database import create_tables; create_tables()"