FROM ghcr.io/astral-sh/uv:0.7.17 AS uv

FROM public.ecr.aws/lambda/python:3.11 AS builder

# Enable bytecode compilation, to improve cold-start performance.
ENV UV_COMPILE_BYTECODE=1

# Disable installer metadata, to create a deterministic layer.
ENV UV_NO_INSTALLER_METADATA=1

# Enable copy mode to support bind mount caching.
ENV UV_LINK_MODE=copy

# Install system dependencies for Chrome
RUN yum update -y && \
  yum install -y wget unzip libX11 libXcomposite libXcursor libXdamage libXext \
  libXi libXtst cups-libs libXScrnSaver libXrandr alsa-lib pango atk at-spi2-atk \
  gtk3 libdrm mesa-libgbm

# Install Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm && \
  yum install -y google-chrome-stable_current_x86_64.rpm && \
  rm google-chrome-stable_current_x86_64.rpm

# # Install uv
# RUN pip install uv

# Bundle the dependencies into the Lambda task root via `uv pip install --target`.
#
# Omit any local packages (`--no-emit-workspace`) and development dependencies (`--no-dev`).
# This ensures that the Docker layer cache is only invalidated when the `pyproject.toml` or `uv.lock`
# files change, but remains robust to changes in the application code.
RUN --mount=from=uv,source=/uv,target=/bin/uv \
  --mount=type=cache,target=/root/.cache/uv \
  --mount=type=bind,source=uv.lock,target=uv.lock \
  --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
  uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt && \
  uv pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"


# Copy source code and uv configuration
# COPY src/ ${LAMBDA_TASK_ROOT}/
# COPY pyproject.toml ${LAMBDA_TASK_ROOT}/
# COPY uv.lock ${LAMBDA_TASK_ROOT}/

# Use uv to build dependencies inside the container
# RUN cd ${LAMBDA_TASK_ROOT} && uv sync

# Copy the runtime dependencies from the builder stage.
# COPY --from=builder ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}/app

# Copy the application code.
COPY ./src ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler function
CMD ["handler.lambda_handler"]
