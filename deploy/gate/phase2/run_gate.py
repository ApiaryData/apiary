#!/usr/bin/env python3
"""The phase 2 gate, on a real network topology made of containers.

Design document, section 12, step 2: a test Apiary of a Pi behind a home router,
a Docker host, a Kubernetes pod and a cloud VM forms the expected colonies with
no extra software; every pair within a site connects directly; every cross-site
pair connects directly or through the relay; a revoked key is refused; a Pi booted
without network time keeps a working floor; every Node in a site commits to the
drive through its host.

This builds the topology in docker-compose.yml (NAT routers that drop unsolicited
inbound traffic, a relay on the open network, a symmetric NAT that direct paths
cannot cross), makes the Apiary's key, tokens and configs with the `apiary` CLI as
a Beekeeper would, starts every Node, and checks each item.

    python run_gate.py            # build the image, run the gate, tear down
    python run_gate.py --skip-build --keep

Needs Docker with permission to give containers NET_ADMIN. It does not need a
cluster: the "Kubernetes pod" is a container dialling out of its own NAT, which
is what a pod does; the StatefulSet manifest is in deploy/k8s.
"""

import argparse
import json
import os
import pathlib
import subprocess
import sys
import textwrap
import time

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[2]
WORK = HERE / "work"
COMPOSE = HERE / "docker-compose.yml"
IMAGE = "apiary-gate:latest"
BASE_IMAGE = "apiary-gate-base:latest"
# RELAY_TLS=1 runs the relay with TLS and QUIC address discovery, which lets two
# Nodes behind ordinary NATs punch through to a direct path. LAB_NAT=MASQ makes the
# pod's NAT an ordinary (cone) one; the default, RANDOM, is a symmetric NAT.
TLS = os.environ.get("RELAY_TLS") == "1"
LAB_NAT = os.environ.get("LAB_NAT", "RANDOM")
RELAY = "https://10.20.0.20:3341" if TLS else "http://10.20.0.20:3340"
PUBLIC_KEY_FILE = WORK / "apiary.key"

# name -> (site, colony, ip, UDP address others may dial directly, serves the comb)
NODES = {
    "cloud": dict(site="cloud", colony="cloud", ip="10.20.0.20", public=True),
    "dockerhost": dict(site="docker-site", colony="lab", ip="10.20.0.30", public=True),
    "k8s-pod": dict(site="k8s-site", colony="lab", ip="10.30.0.11", public=False),
    "pi-host": dict(site="pi-site", colony="edge", ip="10.10.0.11", public=False, comb_host=True),
    "pi-1": dict(site="pi-site", colony="edge", ip="10.10.0.12", public=False, drive=True),
    "pi-2": dict(site="pi-site", colony="edge", ip="10.10.0.13", public=False, drive=True),
    "pi-late": dict(site="pi-site", colony="edge", ip="10.10.0.14", public=False, drive=True),
}
PI_SITE = ["pi-host", "pi-1", "pi-2", "pi-late"]


def sh(args, check=True, env=None, input=None, timeout=900):
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, **(env or {})},
        input=input,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(map(str, args))} failed:\n{result.stdout}\n{result.stderr}")
    return result


def compose(*args, env=None, check=True, input=None):
    return sh(["docker", "compose", "-f", str(COMPOSE), *args], check=check, env=env, input=input)


def docker_path(p):
    return pathlib.Path(p).resolve().as_posix()


def apiary_oneshot(*args, node=None, network=None, extra_mounts=()):
    """Run the apiary CLI in a throwaway container."""
    cmd = ["docker", "run", "--rm", "--user", "root", "--entrypoint", "apiary"]
    if network:
        cmd += ["--network", network]
    if node:
        cmd += ["-v", f"{docker_path(WORK / node)}:/etc/apiary:ro", "-v", f"{docker_path(WORK / node / 'data')}:/data"]
    for host, guest in extra_mounts:
        cmd += ["-v", f"{docker_path(host)}:{guest}"]
    cmd += [IMAGE, *args]
    return sh(cmd).stdout.strip()


def python_in(node, script):
    """Run Python inside a node's container (it has pyarrow and the apiary client)."""
    return compose("exec", "-T", node, "python3", "-", input=textwrap.dedent(script)).stdout


def status(node):
    out = compose("exec", "-T", node, "apiary", "net", "status", "--json", check=False)
    if out.returncode != 0:
        return None
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return None


def wait(what, check, timeout=120, every=2):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = check()
        if last:
            return last
        time.sleep(every)
    raise TimeoutError(f"timed out after {timeout}s waiting for: {what}")


results = []


def record(name, ok, detail=""):
    results.append({"check": name, "ok": bool(ok), "detail": detail})
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"   ({detail})" if detail else ""))


# ---------------------------------------------------------------------------


def build():
    print("Building the Apiary image (this takes a while the first time)...")
    sh(["docker", "build", "-t", BASE_IMAGE, str(ROOT)], timeout=3600)
    sh(
        [
            "docker", "build", "-f", str(HERE / "Dockerfile.gate"), "--build-arg", f"BASE={BASE_IMAGE}",
            "-t", IMAGE, str(HERE),
        ],
        timeout=1800,
    )


def prepare():
    """The Beekeeper's part: the Apiary key, Node keys, tokens and configs."""
    import shutil

    compose("down", "-v", "--remove-orphans", check=False)
    shutil.rmtree(WORK, ignore_errors=True)
    for name in NODES:
        (WORK / name / "data").mkdir(parents=True)

    key_out = apiary_oneshot(
        "key", "generate", "--out", "/work/apiary.key", extra_mounts=[(WORK, "/work")]
    )
    public = key_out.splitlines()[-1].strip()
    print(f"  Apiary public key {public[:16]}...")
    if TLS:
        apiary_oneshot(
            "relay", "cert", "--out", "/work/relay", "--name", "10.20.0.20", "--name", "localhost",
            extra_mounts=[(WORK, "/work")],
        )
        print("  relay certificate made; the relay runs with TLS and address discovery")

    def config(name, token=False):
        n = NODES[name]
        lines = [
            "[node]",
            f'storage = "{n["storage"]}"',
            'cache_dir = "/data/cache"',
            "deposit_interval_secs = 3600",
            "",
            "[flight]",
            'listen = "0.0.0.0:50051"',
            "",
            "[net]",
            'apiary = "gate"',
            f'apiary_public_key = "{public}"',
            f'site = "{n["site"]}"',
            f"udp_port = {7000 if n['public'] else 7001 + list(NODES).index(name)}",
            f'relays = ["{RELAY}"]',
            "mdns = true",
            "discovery_interval_secs = 2",
            "measure_interval_secs = 3",
        ]
        if token:
            lines.append('token_file = "/etc/apiary/token"')
        if n.get("comb_host"):
            lines.append("serve_comb = true")
        if TLS:
            lines.append('relay_ca = "/etc/apiary/relay.pem"')
        if name == "cloud" and not TLS:
            lines.append('serve_relay = "0.0.0.0:3340"')
        if name == "cloud" and TLS:
            lines += [
                "",
                "[net.serve_relay_tls]",
                'https = "0.0.0.0:3341"',
                'quic = "0.0.0.0:7842"',
                'cert = "/etc/apiary/relay.pem"',
                'key = "/etc/apiary/relay.key"',
            ]
        (WORK / name / "apiary.toml").write_text("\n".join(lines) + "\n")
        if TLS:
            import shutil as _shutil

            _shutil.copy(WORK / "relay" / "relay.pem", WORK / name / "relay.pem")
            if name == "cloud":
                _shutil.copy(WORK / "relay" / "relay.key", WORK / name / "relay.key")

    # Node ids come from the keys, which come before the tokens that name them.
    for name in NODES:
        NODES[name]["storage"] = "local:///data/comb"
        config(name)
    for name in NODES:
        NODES[name]["id"] = apiary_oneshot("node", "id", "--config", "/etc/apiary/apiary.toml", node=name)
    host_id = NODES["pi-host"]["id"]
    for name, n in NODES.items():
        if n.get("drive"):
            n["storage"] = f"apiary-drive://{host_id}/"
    for name in NODES:
        config(name, token=True)

    # Tokens: each Node is told every other Node's id and the relay; the public
    # ones also carry the address they can be dialled at.
    for name, n in NODES.items():
        args = [
            "token", "issue", "--key", "/work/apiary.key", "--apiary", "gate",
            "--colony", n["colony"], "--caps", "run,ingest", "--days", "30", "--relay", RELAY,
        ]
        for other, o in NODES.items():
            if other == name:
                continue
            addr = f"@{o['ip']}:7000" if o["public"] else "@"
            args += ["--bootstrap", f"{o['id']}{addr}"]
        token = apiary_oneshot(*args, extra_mounts=[(WORK, "/work")])
        (WORK / name / "token").write_text(token + "\n")
    return public


def start():
    print("Starting the topology...")
    compose("up", "-d", env={"PI_LATE_FAKETIME": "@1970-01-01 00:00:05"})
    for name in NODES:
        wait(f"{name} to answer", lambda n=name: status(n), timeout=180)
    print("  every node is up")


# ---------------------------------------------------------------------------


def peers_of(name):
    s = status(name)
    return {p["id"]: p for p in s["peers"]} if s else {}


# With a TLS relay, a Node whose clock reads 1970 cannot check the relay's certificate
# (it is "not yet valid"), so it cannot use the relay, and the one NATed Node that can
# only be reached through the relay is out of its reach. Everything else still works.
UNREACHABLE = {frozenset(("pi-late", "k8s-pod"))} if TLS else set()


def check_mesh():
    ids = {n: v["id"] for n, v in NODES.items()}
    started = time.time()

    def full():
        for name in NODES:
            have = peers_of(name)
            want = {i for n, i in ids.items() if n != name and frozenset((n, name)) not in UNREACHABLE}
            if not want <= set(have):
                return False
        return True

    try:
        wait("a full mesh", full, timeout=180)
        record("every node joins every other node, with no software beyond the one binary", True,
               f"{time.time() - started:.0f}s")
    except TimeoutError:
        for name in NODES:
            missing = [n for n, i in ids.items() if n != name and i not in peers_of(name) and frozenset((n, name)) not in UNREACHABLE]
            print(f"      {name} is missing {missing}")
        record("every node joins every other node", False, "see above")
        return

    if UNREACHABLE:
        print("      (pi-late and k8s-pod cannot meet: with a 1970 clock pi-late cannot verify the relay's certificate)")

    # The colonies each node reports are the ones its token named.
    for name, n in NODES.items():
        s = status(name)
        record(f"{name} is in colony '{n['colony']}'", s and s["colony"] == n["colony"])

    # Paths.
    matrix = {}
    for name in NODES:
        for p in peers_of(name).values():
            other = next(n for n, v in NODES.items() if v["id"] == p["id"])
            matrix[(name, other)] = p["path"]
    print("\n      path matrix (row's view of column):")
    names = list(NODES)
    print("      " + "".join(f"{n[:10]:>12}" for n in names))
    for a in names:
        print(f"      {a:<10}" + "".join(f"{matrix.get((a, b), '-'):>12}" for b in names))

    in_site = [(a, b) for a in PI_SITE for b in PI_SITE if a != b]
    bad = [(a, b, matrix.get((a, b))) for a, b in in_site if matrix.get((a, b)) != "direct"]
    record("every pair within the Pi site connects directly", not bad, str(bad) if bad else "")
    far = [(a, b, p) for (a, b), p in matrix.items() if p not in ("direct", "relayed")]
    record("every cross-site pair connects directly or through the relay", not far, str(far) if far else "")

    pi_pod = {matrix.get((a, "k8s-pod")) for a in PI_SITE if a != "pi-late"}
    if TLS and LAB_NAT == "MASQ":
        record("two sites behind ordinary NATs punch through to a direct path", pi_pod == {"direct"}, str(pi_pod))
    elif TLS:
        record("a pod behind a symmetric NAT still reaches the Pis, through the relay", pi_pod == {"relayed"}, str(pi_pod))
    else:
        record("a pod behind a symmetric NAT reaches the Pis through the relay", pi_pod == {"relayed"}, str(pi_pod))
    direct_public = all(matrix.get((a, p)) == "direct" for a in PI_SITE for p in ("cloud", "dockerhost"))
    record("Pis behind their router reach the public nodes directly", direct_public)

    # Sites: declared labels, checked against what was measured.
    s = status("pi-1")
    mine = {p["id"]: p for p in s["peers"]}
    same = [p for p in mine.values() if p["same_site"]]
    record("a Pi counts exactly the other Pis as its site", len(same) == 3 and all(p["site"] == "pi-site" for p in same))
    flagged = [p["verdict"] for p in mine.values() if p["verdict"].startswith("contradiction")]
    record("no site label is contradicted by the measured links", not flagged, str(flagged))


def check_drive():
    python_in("pi-host", """
        import apiary
        c = apiary.connect("grpc://127.0.0.1:50051")
        c.create_hive("plant"); c.create_box("plant", "line")
        c.create_frame("plant", "line", "readings", {"id": "int64", "temp": "float64"})
    """)
    ingest = """
        import apiary, pyarrow as pa
        c = apiary.connect("grpc://127.0.0.1:50051")
        ids = list(range({lo}, {lo} + 100))
        c.ingest("plant", "line", "readings", pa.table({{"id": ids, "temp": [1.0] * 100}}))
        print(c.flush_crop())
    """
    for node, lo in (("pi-1", 0), ("pi-2", 1000)):
        python_in(node, ingest.format(lo=lo))
    count = """
        import apiary
        c = apiary.connect("grpc://127.0.0.1:50051")
        t = c.sql("SELECT count(id) AS n FROM plant.line.readings")
        print(t.column(0)[0].as_py())
    """
    counts = {n: python_in(n, count).strip() for n in ("pi-host", "pi-1", "pi-2")}
    record("two Pis commit through the host and every Pi reads all the rows", set(counts.values()) == {"200"}, str(counts))
    on_host = compose("exec", "-T", "pi-host", "sh", "-c",
                      "find /data/comb/plant -name '*.parquet' | wc -l").stdout.strip()
    on_pi1 = compose("exec", "-T", "pi-1", "sh", "-c",
                     "find /data -name '*.parquet' 2>/dev/null | wc -l").stdout.strip()
    record("the data is on the host's drive and not on the client Pi", int(on_host) >= 2 and on_pi1 == "0",
           f"host {on_host} files, pi-1 {on_pi1} files")


def check_clock():
    pl = peers_of("pi-late")
    record("a Pi booted at 1970 joined the colony", len(pl) >= (5 if TLS else 6))
    suspect = [p for p in pl.values() if p["clock_suspect"]]
    record("it noticed its clock was behind its peers' tokens and flagged them", len(suspect) >= 1,
           f"{len(suspect)} of {len(pl)} peers flagged")
    others_see = [s for n in ("pi-host", "cloud") for s in [peers_of(n).get(NODES["pi-late"]["id"])] if s]
    record("the other nodes admit it (their clocks are right)", len(others_see) == 2)

    out = python_in("pi-late", """
        import apiary, pyarrow as pa
        c = apiary.connect("grpc://127.0.0.1:50051")
        print("landed", c.ingest("plant", "line", "readings", pa.table({"id": list(range(5000, 5010)), "temp": [2.0] * 10})))
        try:
            c.flush_crop()
            print("flushed")
        except Exception as e:
            print("refused:", e)
    """)
    record("with no network time it still takes deposits into its crop", "landed 10" in out, out.strip()[:120])
    record("but it refuses to commit until its clock is right", "refused:" in out and "clock" in out.lower(), out.strip()[-160:])

    # The clock is fixed (the Pi gets network time): restart it with the real one.
    compose("up", "-d", "--force-recreate", "pi-late", env={"PI_LATE_FAKETIME": ""})
    wait("pi-late to answer again", lambda: status("pi-late"), timeout=180)
    flushed = wait("pi-late to ship its crop", lambda: _try_flush("pi-late"), timeout=120, every=3)
    record("once the clock is right it ships what it held", flushed is not None, str(flushed)[:100])
    n = python_in("pi-host", """
        import apiary
        c = apiary.connect("grpc://127.0.0.1:50051")
        print(c.sql("SELECT count(id) AS n FROM plant.line.readings WHERE id >= 5000").column(0)[0].as_py())
    """).strip()
    record("and those rows reach the comb on the host", n == "10", n)


def _try_flush(node):
    try:
        out = python_in(node, """
            import apiary
            print(apiary.connect("grpc://127.0.0.1:50051").flush_crop())
        """)
        return out.strip() or None
    except Exception:
        return None


def check_revocation():
    victim = "dockerhost"
    vid = NODES[victim]["id"]
    assert vid in peers_of("cloud") and vid in peers_of("pi-host")
    apiary_oneshot(
        "revoke", "--key", "/work/apiary.key", "--list", "/work/revocations.json", "--node", vid,
        "--push", "http://10.20.0.20:50051",
        network="apiary-gate2_wan", extra_mounts=[(WORK, "/work")],
    )

    def gone():
        return all(vid not in peers_of(n) for n in NODES if n != victim)

    try:
        wait("the revoked node to be cut off everywhere", gone, timeout=90)
        record("a revoked key is cut off from every node, from one push to one node", True)
    except TimeoutError:
        record("a revoked key is cut off from every node", False,
               str({n: vid in peers_of(n) for n in NODES if n != victim}))
        return
    time.sleep(8)  # several discovery rounds: it keeps trying and keeps being refused
    still_out = all(vid not in peers_of(n) for n in NODES if n != victim)
    record("and it stays out: its dials are refused", still_out)
    vs = status(victim)
    refused = [d for d in vs["discovered"] if d["last_error"] and "revoked" in d["last_error"]]
    record("the revoked node is told why", len(refused) >= 1, (refused[0]["last_error"][:100] if refused else "no refusal reason"))
    seqs = {n: status(n)["revocations_seq"] for n in NODES if n != victim}
    record("every other node holds the revocation list", set(seqs.values()) == {1}, str(seqs))
    others_ok = all(len(peers_of(n)) >= 4 for n in ("pi-host", "pi-1", "cloud"))
    record("everyone else is still connected to each other", others_ok)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep", action="store_true", help="leave the topology running")
    args = parser.parse_args()

    if not args.skip_build:
        build()
    print("Preparing keys, tokens and configs...")
    prepare()
    try:
        start()
        print("\nColonies and paths")
        check_mesh()
        print("\nThe drive")
        check_drive()
        print("\nA Pi booted without network time")
        check_clock()
        print("\nRevocation")
        check_revocation()
    finally:
        (HERE / "gate-results.json").write_text(json.dumps(results, indent=2))
        if not args.keep:
            compose("down", "-v", "--remove-orphans", check=False)
    failed = [r for r in results if not r["ok"]]
    print(f"\n{len(results) - len(failed)} of {len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
