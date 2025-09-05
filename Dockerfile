# Multi-stage Dockerfile for Experiment Orchestrator

# Build stage
FROM python:3.13-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt requirements-test.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install -r requirements-test.txt

# Production stage
FROM python:3.13-slim as production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create non-root user
RUN groupadd -r orchestrator && useradd -r -g orchestrator orchestrator

# Create application directory
WORKDIR /app

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/artifacts /app/runtime /app/modules && \
    chown -R orchestrator:orchestrator /app

# Switch to non-root user
USER orchestrator

# Expose port (if needed for API)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import orchestrator_core; print('Health check passed')" || exit 1

# Default command
CMD ["python", "-m", "orchestrator_core"]

# Development stage
FROM production as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    pytest-benchmark \
    black \
    flake8 \
    isort \
    mypy \
    safety \
    bandit

# Switch back to orchestrator user
USER orchestrator

# Override command for development
CMD ["python", "-m", "pytest", "tests/", "-v"]