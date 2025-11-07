FROM public.ecr.aws/x8v8d7g8/mars-base:latest
WORKDIR /app

# Copy repository contents
COPY . .

# Install Python dependencies using uv (lockfile present)
RUN uv sync --frozen

# Start interactive shell for development/testing
CMD ["/bin/bash"]
