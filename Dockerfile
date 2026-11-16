FROM python:3.10-slim AS builder
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt ./
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt "Cython>=3,<4" setuptools wheel
COPY native ./native
RUN /opt/venv/bin/python native/setup_native.py build_ext --inplace \
    && cp _writer_core*.so /opt/venv/lib/python3.10/site-packages/

FROM python:3.10-slim
ENV PATH="/opt/venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY bench.py make_dashboard.py run_med_matrix.py dashboard.html THIRD_PARTY_LICENSE.txt ./
COPY etlbench ./etlbench
COPY tests ./tests
ENTRYPOINT ["python", "bench.py"]
