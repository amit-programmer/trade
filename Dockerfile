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
    touch /app/trader_bot.log /app/bot_state.json && \
    chown -R botuser:botuser /app

# Copy application code
COPY trader_bot.py /app/

# Switch to non-root user
USER botuser

# Default command
CMD ["python", "trader_bot.py"]
