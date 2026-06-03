FROM python:3.12-slim

# Disable buffering and creation of .pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ============================================================
# System dependencies for Playwright + Chromium
# ============================================================
# Playwright's Chromium needs a large set of native libraries to render
# headless Chrome properly. Without these, the browser will silently fail
# to launch (or worse, crash mid-fetch).
#
# We install them in a single layer to keep image size manageable.
# Reference: `playwright install-deps chromium` would do this, but it's
# slow and pulls extras we don't need. Curated list below.
#
# We DO NOT pre-install Chromium itself here — we do that in a later
# layer after pip-installing playwright (it bundles its own download
# manager that picks the exact browser version matching the Python
# package).
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core graphics
    libnss3 libnspr4 libdbus-1-3 \
    libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 \
    libpango-1.0-0 libcairo2 \
    # Audio (required even for headless — chromium init checks for it)
    libasound2 \
    # Fonts so rendered pages don't end up with tofu boxes
    fonts-liberation fonts-noto-color-emoji \
    # Misc utilities
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# Python dependencies
# ============================================================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ============================================================
# Playwright Chromium browser binary
# ============================================================
# `playwright install chromium` downloads the bundled Chromium build that
# matches the playwright Python package version. This avoids version drift
# between the library and the binary (a common source of obscure bugs).
#
# --with-deps would install OS deps too, but we already did that above
# explicitly for clarity and reproducibility.
RUN playwright install chromium

# ============================================================
# Camoufox (anti-bot Firefox-based browser, used by 7 scrapers)
# ============================================================
# Camoufox is a hardened Firefox build that bypasses fingerprinting-based
# bot detection (PerimeterX, Cloudflare, etc). It's distinct from
# Playwright bundled Firefox — has its own binary (~713MB) downloaded
# via `python -m camoufox fetch`. Used by chalkidikiproperties,
# sousouras-realestate, kanata, halkidikiagency, kassandra-properties,
# blueproperty, greece-halkidiki scrapers.
#
# Without this layer, any AsyncCamoufox() call fails at launch with
# missing browser binary or missing-lib errors.
#
# camoufox python package is already pip-installed via requirements.txt
# above. Here we install Firefox OS deps + download the Camoufox Firefox
# binary into /root/.cache/camoufox (~1.3GB total).
RUN playwright install-deps firefox && rm -rf /var/lib/apt/lists/*
RUN python -m camoufox fetch

# ============================================================
# Application source
# ============================================================
COPY ./src ./src

CMD ["python", "-m", "src.main"]
