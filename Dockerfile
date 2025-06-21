FROM public.ecr.aws/lambda/python:3.11

# Install uv
RUN pip install uv

# Copy source code and uv.toml
COPY src/ ${LAMBDA_TASK_ROOT}/
COPY uv.toml ${LAMBDA_TASK_ROOT}/
COPY uv.lock ${LAMBDA_TASK_ROOT}/


# Use uv to build dependencies inside the container
RUN uv build

# Set the CMD to your handler function
CMD ["handler.lambda_handler"]
