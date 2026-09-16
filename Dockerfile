FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY macro_mcp.py .
COPY index.html .

ENV PORT=8000
EXPOSE 8000

CMD ["python", "macro_mcp.py", "http"]

