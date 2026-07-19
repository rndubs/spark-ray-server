#!/usr/bin/env bash
# Push-to-deploy the orchestrator to the Spark and (re)install it there.
# Run from the Mac repo root. Requires the `spark` git remote (see README).
# By default the Ray head is only started if it is down (a head restart
# kills running jobs); pass --restart to force one.
set -euo pipefail

HOST="${SPARK_HOST:-rwhit}"
RESTART="${1:-}"

GIT_SSH_COMMAND="ssh -4" git push spark HEAD:main
ssh -4 "$HOST" "set -e
cd ~/spark-orchestrator
~/.local/bin/uv pip install --python .venv/bin/python -q -e . 'ray[default]==2.56.1'
mkdir -p ~/.config/systemd/user ~/.config/spark-orchestrator ~/spark-runs
cp systemd/spark-ray.service ~/.config/systemd/user/
[ -f ~/.config/spark-orchestrator/capacity.toml ] || cp config/capacity.example.toml ~/.config/spark-orchestrator/capacity.toml
systemctl --user daemon-reload
systemctl --user enable spark-ray.service >/dev/null 2>&1
if [ '$RESTART' = '--restart' ]; then
  systemctl --user restart spark-ray.service
elif ! systemctl --user is-active --quiet spark-ray.service; then
  systemctl --user start spark-ray.service
fi
sleep 3
systemctl --user is-active spark-ray.service"
echo "deployed; spark-ray.service active"
