#!/bin/sh
# Deploy the Prexels image proxy.
#
# Originals are read and written in one S3 bucket. Non-image API requests are
# forwarded to IMAGE_PROXY_BACKEND_URL. Image GET/PUT and /metrics stay here.
#
#   cp .env.example .env    # then fill in the bucket, token, and backend
#   ./deploy.sh             # build and start
#   ./deploy.sh down        # stop

set -eu

cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose v2 is required" >&2
    exit 1
fi

if [ "${1:-up}" = "down" ]; then
    if [ ! -f .env ]; then
        echo "No .env in $(pwd), nothing to stop" >&2
        exit 1
    fi
    docker compose --env-file .env down
    exit 0
fi

if [ "${1:-up}" != "up" ]; then
    echo "usage: $0 [up|down]" >&2
    exit 1
fi

if [ ! -f .env ]; then
    cp .env.example .env
    echo "Wrote $(pwd)/.env from .env.example. Fill in the Prexels values, then run this again." >&2
    exit 1
fi

# Last KEY=value in .env. Does not execute the file. Values may be quoted.
value_of() {
    key=$1
    sed -n "s/^${key}=//p" .env | tail -n 1 | sed 's/^["'\'']//; s/["'\'']$//'
}

bucket=$(value_of IMAGE_PROXY_S3_BUCKET)
if [ -z "$bucket" ]; then
    echo "Set IMAGE_PROXY_S3_BUCKET in .env. One bucket holds the Prexels originals." >&2
    exit 1
fi
case $bucket in
    *[/\\]*|*[[:space:]]*)
        echo "IMAGE_PROXY_S3_BUCKET is not a valid bucket name" >&2
        exit 1
        ;;
esac

upload_token=$(value_of IMAGE_PROXY_UPLOAD_TOKEN)
if [ -z "$upload_token" ]; then
    echo "Set IMAGE_PROXY_UPLOAD_TOKEN in .env. PUT stays disabled until this bearer token is set." >&2
    exit 1
fi
case $upload_token in
    replace-me)
        echo "Replace IMAGE_PROXY_UPLOAD_TOKEN in .env with a long random token." >&2
        exit 1
        ;;
    *[[:space:]]*)
        echo "IMAGE_PROXY_UPLOAD_TOKEN must be a single token with no whitespace." >&2
        exit 1
        ;;
esac

backend=$(value_of IMAGE_PROXY_BACKEND_URL)
if [ -z "$backend" ]; then
    echo "Set IMAGE_PROXY_BACKEND_URL in .env. Non-image API calls are forwarded to this origin." >&2
    exit 1
fi
case $backend in
    http://*|https://*) ;;
    *)
        echo "IMAGE_PROXY_BACKEND_URL must start with http:// or https://" >&2
        exit 1
        ;;
esac
case $backend in
    *prexels.example*|*example.com*)
        echo "Replace IMAGE_PROXY_BACKEND_URL with the Prexels API origin." >&2
        exit 1
        ;;
esac

access_key=$(value_of IMAGE_PROXY_S3_ACCESS_KEY_ID)
secret_key=$(value_of IMAGE_PROXY_S3_SECRET_ACCESS_KEY)
security_token=$(value_of IMAGE_PROXY_S3_SECURITY_TOKEN)
session_token=$(value_of IMAGE_PROXY_S3_SESSION_TOKEN)

if [ -n "$access_key$secret_key$security_token$session_token" ]; then
    if [ -z "$access_key" ] || [ -z "$secret_key" ]; then
        echo "S3 static credentials need both IMAGE_PROXY_S3_ACCESS_KEY_ID and IMAGE_PROXY_S3_SECRET_ACCESS_KEY. The security token is optional. Leave all of them unset to use the default AWS credential chain." >&2
        exit 1
    fi
fi

docker compose --env-file .env up -d --build

cat <<EOF
Prexels image proxy is up.

  images   curl -fsS 'http://127.0.0.1:8000/photo.jpg?size=400'
  upload   curl -fsS -X PUT -H "Authorization: Bearer \$IMAGE_PROXY_UPLOAD_TOKEN" --data-binary @photo.jpg http://127.0.0.1:8000/photo.jpg
  api      curl -fsS http://127.0.0.1:8000/v1/health
  metrics  curl -fsS http://127.0.0.1:8000/metrics

Bucket ${bucket}. Image GET and PUT use that bucket. /metrics stays on this process. Other requests are forwarded to the backend.
EOF
