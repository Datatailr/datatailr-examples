#!/usr/bin/env bash
# Image customisation for the vLLM service. Runs as root in a minimal Debian
# build container before `pip install -r requirements.txt`.
set -euo pipefail

apt-get update
# git: some HF repos pull via git-lfs metadata; libgomp1: OpenMP runtime used by
# torch/vLLM CPU kernels; ca-certificates: HTTPS downloads from the Hub;
# gcc/libc6-dev: Triton JIT-compiles its CUDA driver shim and kernels at runtime
# and needs a C compiler in the *runtime* image.
apt-get install -y --no-install-recommends git libgomp1 ca-certificates gcc libc6-dev
rm -rf /var/lib/apt/lists/*
