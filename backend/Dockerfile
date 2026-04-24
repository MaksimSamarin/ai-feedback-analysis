ARG PYTHON_BASE_IMAGE=python:3.12-slim
FROM ${PYTHON_BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG PIP_TRUSTED_HOST=pypi.org
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" --trusted-host "${PIP_TRUSTED_HOST}" -r /app/requirements.txt

COPY app /app/app
COPY config /app/config

RUN mkdir -p /app/data/uploads /app/results

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
