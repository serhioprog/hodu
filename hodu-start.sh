#!/bin/bash
set -e

# ====================================================================
# Hodu Tunnel Refresh
# ====================================================================
# Kills any running cloudflared, starts a fresh Quick Tunnel, pushes
# the new URL to the Cloudflare Worker's KV so the stable
# hodu.sssukhomlyn.workers.dev URL keeps forwarding here.
#
# APP_URL in .env stays static (the Worker URL) — Hodu generates magic
# links and report URLs against it once, and they remain valid across
# tunnel rotations. The container is NOT restarted on every tunnel
# refresh — only on actual code/env changes.
# ====================================================================

# Load CF credentials from .env (CF_ACCOUNT_ID, CF_KV_NAMESPACE, CF_API_TOKEN)
set -a
source .env
set +a

echo "🛑 Stopping any running cloudflared..."
taskkill //F //IM cloudflared.exe 2>/dev/null || echo "  (no cloudflared running)"
sleep 1

echo "▶  Starting Cloudflare Tunnel..."
> cloudflared.log
./cloudflared.exe tunnel --url http://localhost:8000 > cloudflared.log 2>&1 &

# Wait up to 30s for tunnel URL to appear in log
URL=""
for i in {1..30}; do
    URL=$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' cloudflared.log | head -n1)
    [ -n "$URL" ] && break
    sleep 1
done

if [ -z "$URL" ]; then
    echo "❌ Tunnel URL not found in cloudflared.log after 30s"
    echo "   Check: tail -f cloudflared.log"
    exit 1
fi

echo "✅ Tunnel URL: $URL"

# Push to Worker KV via Cloudflare API
echo "📡 Pushing tunnel URL to Worker KV..."
if [ -z "$CF_API_TOKEN" ] || [ -z "$CF_ACCOUNT_ID" ] || [ -z "$CF_KV_NAMESPACE" ]; then
    echo "⚠️  CF API credentials missing in .env — update KV manually:"
    echo "    Workers & Pages → KV → HODU_CONFIG"
    echo "    key: tunnel_url"
    echo "    value: $URL"
else
    RESPONSE=$(curl -sS -X PUT \
        "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE}/values/tunnel_url" \
        -H "Authorization: Bearer ${CF_API_TOKEN}" \
        -H "Content-Type: text/plain" \
        --data "$URL")
    if echo "$RESPONSE" | grep -q '"success":true'; then
        echo "✅ Worker KV updated"
    else
        echo "❌ KV update failed: $RESPONSE"
    fi
fi

# Verify hodu_scraper is up (does NOT restart — APP_URL hasn't changed)
SCRAPER_STATUS=$(docker inspect --format='{{.State.Status}}' hodu_scraper 2>/dev/null || echo "missing")
if [ "$SCRAPER_STATUS" != "running" ]; then
    echo "⚠️  hodu_scraper is $SCRAPER_STATUS — starting it"
    docker compose up -d scraper
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  ✅ Hodu Tunnel Ready"
echo "════════════════════════════════════════════════════════════"
echo "  🌐 Stable URL:    https://hodu.sssukhomlyn.workers.dev"
echo "  🌪️  Tunnel URL:    $URL"
echo "  🏠 Admin (local): http://localhost:8000/admin"
echo "  📜 Tunnel logs:   tail -f cloudflared.log"
echo "  🛑 Stop tunnel:   taskkill //F //IM cloudflared.exe"
echo "════════════════════════════════════════════════════════════"
echo "💡 Magic links use the stable Worker URL — no need to regenerate"
echo "   them after tunnel restart. Just re-run this script if tunnel"
echo "   drops, KV gets the new URL automatically."