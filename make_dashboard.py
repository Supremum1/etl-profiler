"""Build a portable HTML report from a directory of ETL resource profiles."""
import argparse
import json
from bisect import bisect_left
from pathlib import Path

from run_med_matrix import configurations

ROOT = Path(__file__).resolve().parent
SERVICES = ("python", "postgres", "clickhouse")
COUNTERS = ("cpu_s", "read", "write", "rx", "tx", "throttled_s")


def boundary(samples, times, key, t):
    index = bisect_left(times, t)
    if index < len(times) and times[index] == t:
        return samples[index][1].get(key)
    if not 0 < index < len(times):
        return None
    a, b = samples[index - 1][1].get(key), samples[index][1].get(key)
    if a is None or b is None or b < a:
        return None
    return a + (b - a) * (t - times[index - 1]) / (times[index] - times[index - 1])


def summarize(samples, duration):
    times = [t for t, _ in samples]
    result = {}
    for key in COUNTERS:
        a, b = boundary(samples, times, key, 0), boundary(samples, times, key, duration)
        result[key] = round(b - a, 6) if a is not None and b is not None and b >= a else None
    for key in ("memory", "pids"):
        observations = [values[key] for t, values in samples if 0 <= t <= duration and key in values]
        result[key] = max(observations, default=None)
    return result


def build(series, output):
    runs = []
    for path in sorted(series.glob("*.profile.json")):
        run = json.loads(path.read_text(encoding="utf-8"))
        run["configuration"], _, run["repeat"] = path.name.removesuffix(".profile.json").rpartition("-r")
        run["summary"] = {
            name: summarize(run["samples"][name], run["duration"]) for name in SERVICES
        }
        runs.append(run)
    metadata = series / "environment.json"
    environment = json.loads(metadata.read_text(encoding="utf-8")) if metadata.exists() else {}
    payload = {
        "environment": environment,
        "runs": runs,
        "logs": len(list(series.glob("*.log"))),
        "planned_configurations": environment.get("planned_configurations", len(list(configurations())))
        if environment else None,
    }
    template = (ROOT / "dashboard.html").read_text(encoding="utf-8")
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(template.replace("__REPORT_DATA__", data, 1), encoding="utf-8")
    print(f"{output} ({len(runs)} profiles)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("series", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    build(args.series, args.output or args.series / "dashboard.html")
