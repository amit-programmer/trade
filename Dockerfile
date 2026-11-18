# =============================================================================
# Production Dockerfile for Trader Bot (Liquidity Sweep + SMC Scanner)
# =============================================================================
FROM python:3.11-slim

# Prevent Python from writing .pyc files & buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MPLBACKEND=Agg

# Set work directory
WORKDIR /app

# Install minimal OS dependencies for matplotlib/mplfinance rendering and SSL certs
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    fonts-dejavu-core \
    libfreetype6 \
    libpng16-16 \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies first (cached layer)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-privileged user and setup data files
RUN useradd -m -u 1001 botuser && \
    mkdir -p /app && \
    touch /app/trader_bot.log /app/bot_state.json /app/heartbeat.json /app/paper_positions.json /app/crash_dump.json && \
    chmod 666 /app/trader_bot.log /app/bot_state.json /app/heartbeat.json /app/paper_positions.json /app/crash_dump.json && \
    chown -R botuser:botuser /app

# Copy application code, modular engines and dashboard templates
COPY *.py /app/
COPY templates /app/templates

# Switch to non-root user
USER botuser

# Expose Web Dashboard Port
EXPOSE 8000

# Docker Healthcheck instruction for autonomous auto-restart
HEALTHCHECK --interval=60s --timeout=10s --retries=3 CMD curl -f http://localhost:8000/health || python trader_bot.py --health || exit 1

# Default command (starts Web Dashboard + 24/7 Scanner in parallel)
CMD ["python", "server.py", "--all"]
