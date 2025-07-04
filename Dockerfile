FROM ghcr.io/astral-sh/uv:0.7.17 AS uv

FROM public.ecr.aws/lambda/python:3.11

# Enable bytecode compilation for cold-start performance.
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_INSTALLER_METADATA=1
ENV UV_LINK_MODE=copy

# Install system dependencies for Chrome and Chromedriver
RUN yum update -y && \
  yum install -y wget unzip libX11 libXcomposite libXcursor libXdamage libXext \
  libXi libXtst cups-libs libXScrnSaver libXrandr alsa-lib pango atk at-spi2-atk \
  gtk3 libdrm mesa-libgbm

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm && \
  yum install -y ./google-chrome-stable_current_x86_64.rpm && \
  rm google-chrome-stable_current_x86_64.rpm

# Get matching Chromedriver version
RUN CHROME_VERSION=$(google-chrome-stable --version | grep -oP "\d+\.\d+\.\d+\.\d+") && \
  echo "Chrome version: $CHROME_VERSION" && \
  CHROMEDRIVER_VERSION=$(curl -sS "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json" | \
  python3 -c "import sys, json; print(json.load(sys.stdin)['channels']['Stable']['version'])") && \
  echo "Matching Chromedriver version: $CHROMEDRIVER_VERSION" && \
  wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$CHROMEDRIVER_VERSION/linux64/chromedriver-linux64.zip" && \
  unzip chromedriver-linux64.zip && \
  mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
  chmod +x /usr/bin/chromedriver && \
  rm -rf chromedriver-linux64 chromedriver-linux64.zip

# Install Python dependencies using uv
RUN --mount=from=uv,source=/uv,target=/bin/uv \
  --mount=type=cache,target=/root/.cache/uv \
  --mount=type=bind,source=uv.lock,target=uv.lock \
  --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
  uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt && \
  uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy source code
COPY ./src ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler function
CMD ["handler.lambda_handler"]
