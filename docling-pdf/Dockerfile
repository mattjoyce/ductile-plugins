# Slim python image for the docling-pdf plugin, per claude-fro.
#
# Two deployment models are possible (decided at deploy time, see
# README "Deployment"):
#   A) plugin runs in-process inside the ductile container (firecrawl
#      model) — then these deps live in ductile's own image and this
#      Dockerfile is unused.
#   B) ductile shells into this image per job (marker model) — then this
#      image is built on unraid and ductile invokes `run.py` inside it.
#
# This Dockerfile supports model (B). CPU wheels by default; swap the
# torch line for a CUDA build to use the unraid RTX 2070.

FROM python:3.12-slim

# docling needs a few system libs for PDF/image handling.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /plugin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run.py polish_prompt_v1.txt ./

# The plugin is a stdin/stdout JSON filter — ductile pipes the request
# in and reads the response out.
ENTRYPOINT ["python", "run.py"]
