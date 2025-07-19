ARG FUNCTION_DIR="/app"

FROM ghcr.io/astral-sh/uv:0.7.17 AS uv
FROM mcr.microsoft.com/playwright/python:v1.54.0

ARG FUNCTION_DIR

ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_INSTALLER_METADATA=1
ENV UV_LINK_MODE=copy
ENV UV_PIP_ONLY_BINARY=:all:
ENV PIP_ONLY_BINARY=:all:

RUN --mount=from=uv,source=/uv,target=/bin/uv \
  --mount=type=cache,target=/root/.cache/uv \
  --mount=type=bind,source=uv.lock,target=uv.lock \
  --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
  uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt && \
  uv pip install -r requirements.txt --target ${FUNCTION_DIR}

RUN mkdir -p ${FUNCTION_DIR}
COPY ./src ${FUNCTION_DIR}

RUN pip install \
  --target ${FUNCTION_DIR} \
  awslambdaric

WORKDIR ${FUNCTION_DIR}

ENTRYPOINT [ "/usr/bin/python", "-m", "awslambdaric" ]

CMD ["collector.lambda_handler"]
