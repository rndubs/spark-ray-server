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
~/.local/bin/uv pip install --python .venv/bin/python -q -e '.[dashboard]' 'ray[default]==2.56.1'
mkdir -p ~/.config/systemd/user ~/.config/spark-orchestrator ~/spark-runs
cp systemd/spark-ray.service systemd/spark-dashboard.service systemd/spark-admit.service ~/.config/systemd/user/
[ -f ~/.config/spark-orchestrator/capacity.toml ] || cp config/capacity.example.toml ~/.config/spark-orchestrator/capacity.toml
mkdir -p ~/spark-runs/queue/pending ~/spark-runs/queue/admitted
systemctl --user daemon-reload
systemctl --user enable spark-ray.service spark-dashboard.service spark-admit.service >/dev/null 2>&1
if [ '$RESTART' = '--restart' ]; then
  systemctl --user restart spark-ray.service
elif ! systemctl --user is-active --quiet spark-ray.service; then
  systemctl --user start spark-ray.service
fi
# The dashboard is read-only (kills no jobs), so always bounce it to pick up
# new code — independent of the Ray head restart guard above.
systemctl --user restart spark-dashboard.service
# The admitter holds no job state in memory (the queue is on disk) and does not
# touch running jobs, so bouncing it is safe mid-campaign: the worst case is a
# few seconds where nothing is admitted.
systemctl --user restart spark-admit.service
sleep 3
echo \"spark-ray: \$(systemctl --user is-active spark-ray.service)\"
echo \"spark-dashboard: \$(systemctl --user is-active spark-dashboard.service)\"
echo \"spark-admit: \$(systemctl --user is-active spark-admit.service)\""
echo "deployed; spark-ray + spark-dashboard + spark-admit active"
