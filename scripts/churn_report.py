#!/usr/bin/env python3
"""Churn-latency report for landslide-sqlite.

Builds and runs the Rust probe (landslide-sqlite/examples/churn) against a local
directory object store and — when LANDSLIDE_TEST_BUCKET is set — real S3, then
summarizes the SAMPLE CSV lines it emits:

    uv run --with matplotlib scripts/churn_report.py            # local only
    LANDSLIDE_TEST_BUCKET=my-bucket uv run --with matplotlib \\
        scripts/churn_report.py                                  # + real S3

AWS creds: env vars if present, else pulled from `aws configure get`.
Matplotlib is optional: without it you get the table only.
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target/release/examples/churn"
PNG = Path(__file__).with_name("churn_latency.png")

ROUNDS = os.environ.get("LANDSLIDE_CHURN_ROUNDS", "30")
TXNS = os.environ.get("LANDSLIDE_CHURN_TXNS", "150")


def pct(data, p):
    if not data:
        return float("nan")
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def aws_env(env):
    """Env for the S3 backend: AWS creds in process env win, else ~/.aws via the CLI."""
    if not env.get("LANDSLIDE_TEST_BUCKET"):
        return None
    env = dict(env)
    if "AWS_ACCESS_KEY_ID" not in env:
        get = lambda k: subprocess.run(
            ["aws", "configure", "get", k], capture_output=True, text=True
        ).stdout.strip()
        key, secret = get("aws_access_key_id"), get("aws_secret_access_key")
        if not key or not secret:
            sys.exit("LANDSLIDE_TEST_BUCKET is set but no AWS_ACCESS_KEY_ID in env and no keys in ~/.aws")
        env["AWS_ACCESS_KEY_ID"] = key
        env["AWS_SECRET_ACCESS_KEY"] = secret
    env.setdefault("AWS_REGION", "us-east-1")
    return env


def build():
    subprocess.run(
        ["cargo", "build", "-p", "landslide-sqlite", "--release", "--example", "churn"],
        cwd=ROOT, check=True,
    )


def run(backend, profile, base_env):
    env = dict(base_env)
    env["LANDSLIDE_CHURN_ROUNDS"] = ROUNDS
    env["LANDSLIDE_CHURN_TXNS"] = TXNS
    env["LANDSLIDE_CHURN_PROFILE"] = profile
    if backend == "local":
        env.pop("LANDSLIDE_TEST_BUCKET", None)
    proc = subprocess.run([str(BIN)], env=env, capture_output=True, text=True, check=True)
    samples, verify, name = [], None, None
    for line in proc.stdout.splitlines():
        f = line.split(",")
        if f[0] == "SAMPLE":
            samples.append(dict(phase=f[2], round=int(f[3]), txns=int(f[4]), bytes=int(f[5]), ms=float(f[6])))
        elif f[0] == "VERIFY":
            verify = line
    for line in proc.stderr.splitlines():
        if "name=" in line:
            name = line.split("name=")[1].split()[0]
    return samples, verify, name


def cleanup_s3(names):
    bucket = os.environ["LANDSLIDE_TEST_BUCKET"]
    prefixes = ["churn/"] + [f"ltx/{n}" for n in names]
    for prefix in prefixes:
        subprocess.run(
            ["aws", "s3", "rm", "--recursive", "--quiet", f"s3://{bucket}/{prefix}"],
            check=False,
        )
    print(f"(cleaned {len(prefixes)} prefixes under s3://{bucket})")


def report(backend, samples):
    phases = ["write", "sync", "checkpoint", "open", "open_fresh", "hydrate"]
    by_phase = {}
    for s in samples:
        by_phase.setdefault(s["phase"], []).append(s)
    print(f"\n== {backend} ==  ({ROUNDS} rounds x {TXNS} tiny upserts, autocommit)")
    print(f"{'phase':<11}{'n':>5}{'mean':>9}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}   ms")
    for ph in phases:
        rows = by_phase.get(ph)
        if not rows:
            continue
        ms = [r["ms"] for r in rows]
        print(f"{ph:<11}{len(ms):>5}{sum(ms)/len(ms):>9.1f}{pct(ms,50):>9.1f}{pct(ms,95):>9.1f}{pct(ms,99):>9.1f}{max(ms):>9.1f}")
    w = sum(r["ms"] for r in by_phase.get("write", []))
    s = sum(r["ms"] for r in by_phase.get("sync", []))
    total_txns = sum(r["txns"] for r in by_phase.get("write", []))
    if w:
        print(f"  sqlite exec throughput:        {total_txns / (w / 1e3):,.0f} txns/s")
    if w + s:
        print(f"  durable rate (write+sync):     {total_txns / ((w + s) / 1e3):,.0f} txns/s")
        print(f"  durability latency, amortized: {s / max(total_txns,1):.2f} ms/txn, {s:.0f} ms/round of {TXNS}")


def plot(all_samples):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    axes[0].set_title("durability barrier (sync) per round")
    axes[1].set_title(f"sqlite exec wall per round ({TXNS} txns)")
    for backend, samples in all_samples.items():
        for ax, ph in [(axes[0], "sync"), (axes[1], "write")]:
            pts = [(s["round"], s["ms"]) for s in samples if s["phase"] == ph]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker=".", label=backend)
    for ax in axes:
        ax.set_xlabel("round")
        ax.set_ylabel("ms")
        ax.legend()
    fig.tight_layout()
    fig.savefig(PNG, dpi=120)
    return True


def main():
    build()
    s3 = aws_env(os.environ)
    profiles = os.environ.get("LANDSLIDE_CHURN_PROFILES", "default,fastflush,compact").split(",")
    all_samples = {}
    s3_names = []
    for backend, env in [("local", os.environ), ("s3", s3)]:
        if env is None:
            continue
        for profile in profiles:
            samples, verify, name = run(backend, profile, env)
            label = f"{backend}-{profile}"
            print(f"\n--- {label} run ---", verify or "(no VERIFY line)")
            all_samples[label] = samples
            report(label, samples)
            if name:
                s3_names.append(name)
    if s3_names:
        cleanup_s3(s3_names)
    if plot(all_samples):
        print(f"\nplot written to {PNG}")


if __name__ == "__main__":
    main()
