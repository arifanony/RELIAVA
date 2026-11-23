# One-host platform image: FastAPI intake + embedded relay.
FROM python:3.12-slim
WORKDIR /srv
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir .
EXPOSE 8000
CMD ["python", "-c", "from ai_sre.main import run; run()"]
