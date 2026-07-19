"""Spark training dashboard: a hexgen-aware, read-only overlay on the Ray
orchestrator. Serves one localhost-only web page answering, per training job,
what is running / on what code / on what data / how far along / how healthy /
what it has cost (GPU-hours). Spec: planning/DASHBOARD_SPEC.md."""
