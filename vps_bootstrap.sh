#!/usr/bin/env bash
# =============================================================================
# Ouroboros VPS bootstrap — first-boot git setup, then exec vps_launcher.py
#
# Intended to be the ExecStart of the systemd unit; idempotent on re-runs.
# Expects env vars (from systemd EnvironmentFile or a manual `source .env`):
#   GITHUB_TOKEN, GITHUB_USER, GITHUB_REPO
#   OUROBOROS_REPO_DIR, OUROBOROS_STATE_DIR
#   OUROBOROS_VENV           — path to the python venv (e.g. /home/ouroboros/app/venv)
#   OUROBOROS_BOOT_BRANCH    — optional, default: ouroboros
#
# Manual invocation (debugging):
#   set -a; source /home/ouroboros/app/.env; set +a
#   /home/ouroboros/app/repo/vps_bootstrap.sh
# =============================================================================
set -euo pipefail

: "${GITHUB_TOKEN:?GITHUB_TOKEN not set}"
: "${GITHUB_USER:?GITHUB_USER not set}"
: "${GITHUB_REPO:?GITHUB_REPO not set}"
: "${OUROBOROS_REPO_DIR:?OUROBOROS_REPO_DIR not set}"
: "${OUROBOROS_STATE_DIR:?OUROBOROS_STATE_DIR not set}"
: "${OUROBOROS_VENV:?OUROBOROS_VENV not set}"

BOOT_BRANCH="${OUROBOROS_BOOT_BRANCH:-ouroboros}"
STABLE_BRANCH="${BOOT_BRANCH}-stable"
REMOTE_URL="https://${GITHUB_TOKEN}:x-oauth-basic@github.com/${GITHUB_USER}/${GITHUB_REPO}.git"

mkdir -p "${OUROBOROS_STATE_DIR}"/{state,logs,memory,index,locks,archive}

# Clone on first run; otherwise reattach remote so rotated tokens take effect.
if [[ ! -d "${OUROBOROS_REPO_DIR}/.git" ]]; then
    rm -rf "${OUROBOROS_REPO_DIR}"
    git clone "${REMOTE_URL}" "${OUROBOROS_REPO_DIR}"
else
    git -C "${OUROBOROS_REPO_DIR}" remote set-url origin "${REMOTE_URL}"
fi

cd "${OUROBOROS_REPO_DIR}"
git fetch origin

if git rev-parse --verify "origin/${BOOT_BRANCH}" >/dev/null 2>&1; then
    git checkout "${BOOT_BRANCH}"
    git reset --hard "origin/${BOOT_BRANCH}"
else
    echo "[boot] branch ${BOOT_BRANCH} not found on fork — creating from origin/main"
    git checkout -b "${BOOT_BRANCH}" origin/main
    git push -u origin "${BOOT_BRANCH}"
    git branch "${STABLE_BRANCH}"
    git push -u origin "${STABLE_BRANCH}"
fi

HEAD_SHA="$(git rev-parse HEAD)"
echo "[boot] branch=${BOOT_BRANCH} sha=${HEAD_SHA:0:12}"
echo "[boot] state=${OUROBOROS_STATE_DIR}"
echo "[boot] repo=${OUROBOROS_REPO_DIR}"
echo "[boot] logs: ${OUROBOROS_STATE_DIR}/logs/supervisor.jsonl"

# exec replaces the shell, so the launcher becomes PID 1 of the service.
# systemd can then signal the python process directly on stop/restart.
exec "${OUROBOROS_VENV}/bin/python" "${OUROBOROS_REPO_DIR}/vps_launcher.py"
