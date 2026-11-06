#!/bin/bash
# Install the Neon binaries into the job image.
#
# Neon ships Docker images, not binary tarballs, so this script pulls the image
# filesystems with crane (no Docker daemon needed) and copies out:
#   - pageserver, safekeeper, storage_broker and a Postgres build with the neon
#     extension from ghcr.io/neondatabase/neon
#   - compute_ctl from ghcr.io/neondatabase/compute-node-v<PG_VERSION>
# Binaries land in /opt/neon/bin, Postgres in /opt/neon/v<PG_VERSION>.
# Override NEON_TAG / NEON_PG_VERSION to pin versions.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

NEON_TAG="${NEON_TAG:-latest}"
PG_VERSION="${NEON_PG_VERSION:-17}"
CRANE_VERSION="${CRANE_VERSION:-v0.22.1}"
NEON_DIR=/opt/neon

apt-get update
# curl + ca-certificates for the download; the rest are runtime libraries of the Neon binaries.
apt-get install -y curl ca-certificates libreadline8 libseccomp2 openssl libicu76 zlib1g

ARCH="$(dpkg --print-architecture)"
case "${ARCH}" in
	amd64) CRANE_ARCH=x86_64 ;;
	arm64) CRANE_ARCH=arm64 ;;
	*) echo "unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

curl -sSL "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_${CRANE_ARCH}.tar.gz" \
	| tar -xz -C /usr/local/bin crane

mkdir -p "${NEON_DIR}"

echo "Extracting pageserver, safekeeper, storage_broker and Postgres ${PG_VERSION} from ghcr.io/neondatabase/neon:${NEON_TAG}"
crane export --platform "linux/${ARCH}" "ghcr.io/neondatabase/neon:${NEON_TAG}" - \
	| tar -x -C "${NEON_DIR}" --strip-components=2 \
		usr/local/bin/pageserver usr/local/bin/safekeeper usr/local/bin/storage_broker "usr/local/v${PG_VERSION}"

echo "Extracting compute_ctl from ghcr.io/neondatabase/compute-node-v${PG_VERSION}:${NEON_TAG}"
crane export --platform "linux/${ARCH}" "ghcr.io/neondatabase/compute-node-v${PG_VERSION}:${NEON_TAG}" - \
	| tar -x -C "${NEON_DIR}" --strip-components=2 usr/local/bin/compute_ctl

rm -f /usr/local/bin/crane

# compute_ctl raises the core dump limit to unlimited and aborts with EPERM when the
# container's hard limit is finite, as it is on Datatailr nodes. This shim, preloaded
# into compute_ctl only, turns that one failure into success.
apt-get install -y gcc libc6-dev
mkdir -p "${NEON_DIR}/lib"
cat > /tmp/setrlimit_shim.c <<'CSRC'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <sys/resource.h>

int setrlimit(__rlimit_resource_t res, const struct rlimit *lim)
{
	static int (*real)(__rlimit_resource_t, const struct rlimit *) = 0;
	if (!real)
		real = dlsym(RTLD_NEXT, "setrlimit");
	int rc = real(res, lim);
	if (rc != 0 && res == RLIMIT_CORE && errno == EPERM)
		return 0;
	return rc;
}

int setrlimit64(__rlimit_resource_t res, const struct rlimit64 *lim)
{
	return setrlimit(res, (const struct rlimit *)lim);
}
CSRC
gcc -shared -fPIC -O2 -o "${NEON_DIR}/lib/setrlimit_shim.so" /tmp/setrlimit_shim.c -ldl
rm -f /tmp/setrlimit_shim.c

for binary in bin/pageserver bin/safekeeper bin/storage_broker bin/compute_ctl "v${PG_VERSION}/bin/postgres"; do
	if ldd "${NEON_DIR}/${binary}" | grep -q "not found"; then
		echo "missing shared libraries for ${binary}:" >&2
		ldd "${NEON_DIR}/${binary}" | grep "not found" >&2
		exit 1
	fi
done
"${NEON_DIR}/bin/pageserver" --version
"${NEON_DIR}/v${PG_VERSION}/bin/postgres" --version
