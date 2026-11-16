"""Resource counters from systemd services and their cgroup v2 subtrees."""
import subprocess
from pathlib import Path
from time import perf_counter

CGROUP = Path("/sys/fs/cgroup")


def cgroup_units():
    own_group = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]
    return {
        "python": Path(own_group).name,
        "postgres": "postgresql@14-main.service",
        "clickhouse": "clickhouse-server.service",
    }


class LinuxResources:
    def __init__(self):
        self.units = cgroup_units()
        self.paths = {}
        for name, unit in self.units.items():
            control_group = subprocess.check_output(
                ["systemctl", "show", "--property=ControlGroup", "--value", unit],
                text=True, timeout=2,
            ).strip()
            if not control_group:
                raise RuntimeError(f"{unit} has no active cgroup")
            self.paths[name] = CGROUP / control_group.lstrip("/")

    def sample(self, name):
        path = self.paths[name]
        start = perf_counter()
        cpu = fields(path / "cpu.stat")
        memory = int((path / "memory.current").read_text())
        cache = fields(path / "memory.stat")
        pids = int((path / "pids.current").read_text())
        io = {"rbytes": 0, "wbytes": 0}
        for row in (path / "io.stat").read_text().splitlines():
            for field in row.split()[1:]:
                key, val = field.split("=")
                if key in io:
                    io[key] += int(val)
        timestamp = (start + perf_counter()) / 2
        network = subprocess.check_output(
            ["systemctl", "show", "-p", "IPIngressBytes", "-p", "IPEgressBytes", self.units[name]],
            text=True, timeout=2,
        )
        net = dict(line.split("=", 1) for line in network.splitlines() if "=" in line)
        return timestamp, {
            "cpu_s": cpu["usage_usec"] / 1e6,
            "throttled_s": cpu.get("throttled_usec", 0) / 1e6,
            "memory": max(0, memory - cache.get("inactive_file", 0)),
            "pids": pids,
            "read": io["rbytes"],
            "write": io["wbytes"],
            "rx": integer_or_none(net.get("IPIngressBytes", "")),
            "tx": integer_or_none(net.get("IPEgressBytes", "")),
        }


def fields(path):
    return {key: int(value) for key, value in (row.split() for row in path.read_text().splitlines())}


def integer_or_none(value):
    return int(value) if value.isdecimal() else None
