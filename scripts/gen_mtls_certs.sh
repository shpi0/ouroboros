#!/bin/bash
# Generate mTLS certificates for ouroboros.iarovoivv.com
# Creates: CA, server cert, client cert + .p12 for browser import
#
# Usage: bash scripts/gen_mtls_certs.sh
# Output: certs/ directory in repo root

set -e

DOMAIN="ouroboros.iarovoivv.com"
CERT_DIR="$(cd "$(dirname "$0")/.." && pwd)/certs"
mkdir -p "$CERT_DIR"

echo "[1/6] Generating CA key and certificate (10 years)..."
openssl genrsa -out "$CERT_DIR/ca.key" 4096

openssl req -new -x509 -days 3650 \
  -key "$CERT_DIR/ca.key" \
  -out "$CERT_DIR/ca.crt" \
  -subj "/C=RU/O=Ouroboros/CN=Ouroboros CA"

echo "[2/6] Generating server key..."
openssl genrsa -out "$CERT_DIR/server.key" 2048

echo "[3/6] Generating server CSR and signing with CA (2 years)..."
openssl req -new \
  -key "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.csr" \
  -subj "/C=RU/O=Ouroboros/CN=$DOMAIN"

cat > "$CERT_DIR/server_ext.cnf" <<EOF
[SAN]
subjectAltName=DNS:$DOMAIN,DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -days 730 \
  -in "$CERT_DIR/server.csr" \
  -CA "$CERT_DIR/ca.crt" \
  -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial \
  -out "$CERT_DIR/server.crt" \
  -extfile "$CERT_DIR/server_ext.cnf" \
  -extensions SAN

echo "[4/6] Generating client key..."
openssl genrsa -out "$CERT_DIR/client.key" 2048

echo "[5/6] Generating client CSR and signing with CA (2 years)..."
openssl req -new \
  -key "$CERT_DIR/client.key" \
  -out "$CERT_DIR/client.csr" \
  -subj "/C=RU/O=Ouroboros/CN=Ouroboros Client"

openssl x509 -req -days 730 \
  -in "$CERT_DIR/client.csr" \
  -CA "$CERT_DIR/ca.crt" \
  -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial \
  -out "$CERT_DIR/client.crt"

echo "[6/6] Creating PKCS12 bundle for browser import..."
openssl pkcs12 -export \
  -in "$CERT_DIR/client.crt" \
  -inkey "$CERT_DIR/client.key" \
  -certfile "$CERT_DIR/ca.crt" \
  -out "$CERT_DIR/client.p12" \
  -passout pass:ouroboros

# Clean up temp files
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/client.csr" "$CERT_DIR/server_ext.cnf" "$CERT_DIR/ca.srl"

echo ""
echo "========================================================"
echo "  mTLS certificates generated in: $CERT_DIR"
echo "========================================================"
echo ""
echo "Files created:"
echo "  ca.crt         — CA certificate (trust anchor)"
echo "  ca.key         — CA private key (keep secret!)"
echo "  server.crt     — Server certificate for $DOMAIN"
echo "  server.key     — Server private key"
echo "  client.crt     — Client certificate"
echo "  client.key     — Client private key"
echo "  client.p12     — PKCS12 bundle for browser (password: ouroboros)"
echo ""
echo "Next steps:"
echo "  1. Copy certs/ to your VPS: scp -r certs/ user@$DOMAIN:~/ouroboros/certs/"
echo "  2. Update nginx config paths to match deployment location"
echo "  3. Install nginx config: sudo cp scripts/nginx_ouroboros.conf /etc/nginx/sites-available/ouroboros"
echo "  4. sudo ln -s /etc/nginx/sites-available/ouroboros /etc/nginx/sites-enabled/"
echo "  5. sudo nginx -t && sudo systemctl reload nginx"
echo "  6. Import client.p12 into your browser (password: ouroboros)"
echo "     Chrome: Settings > Privacy > Manage certificates > Import"
echo "     Firefox: Settings > Privacy > View Certificates > Import"
echo ""
echo "IMPORTANT: Keep ca.key and server.key secret. Do not commit them to git."
