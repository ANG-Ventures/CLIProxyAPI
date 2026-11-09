#!/usr/bin/env python3
"""Mutation check for the contentalias lock-hold gates (t_c0059464).

Each mutant re-introduces one member of the t_129cf1ac class (slow work under
the store lock, or a superlinear step on the request/response path), runs the
three gates, records which go RED, and restores the source with `git checkout`.
A mutant that no gate catches is a gap: the script exits 1.

Run from the repo root on a host that may run `go test` (CI or ACE-AI):
    python3 scripts/dpx/contentalias_lockhold_mutants.py
"""
import re
import subprocess
import sys
from pathlib import Path

PKG = Path("internal/contentalias")
GATES = ["TestPrepareLockHoldBudget",
         "TestPreparePerformanceUnderParallelCallers",
         "TestPrepareScalesLinearly",
         "TestWarmPrepareDoesNotWaitForStoreLock",
         "TestPrepareP90FlatAt30Goroutines"]


def sub(pattern, repl, flags=0):
    def f(src):
        out, n = re.subn(pattern, repl, src, count=1, flags=flags)
        if n != 1:
            raise SystemExit(f"mutant anchor not found: {pattern!r}")
        return out
    return f


def chain(*fs):
    def f(src):
        for g in fs:
            src = g(src)
        return src
    return f


MUTANTS = [
    # Pre-#24 encodeText: try the unanchored regexp at every position.
    ("encode_unanchored_scan", "prose.go",
     sub(r'if strings\.HasPrefix\(text\[pos:\], "[^"]*"\) \{\n(\t\t\tif loc := codecPattern)', r"if true {\n\1")),
    # Card mutant 2: a 50 ms wait under the lock (in the commit, the only hold left).
    ("sleep_50ms_under_lock", "session.go",
     sub(r"(func \(s \*Session\) commitIfUnchanged[^\n]*\n(?:.*\n)*?\tdefer unlock\(lock\)\n)",
         r"\1\ttime.Sleep(50 * time.Millisecond)\n")),
    # Serialization the store-lock hold cannot see: a process-wide mutex around
    # Prepare's work (t_dae16eeb shape; the pre-t_1f4b978a design). The hold
    # hook does not time it, so only the 30-goroutine p90 bound catches it.
    ("serialize_outside_flock", "request.go",
     chain(sub(r"(\tfor attempt := 0; attempt < optimisticAttempts; attempt\+\+ \{\n)",
               r"\tmutantMu.Lock()\n\tdefer mutantMu.Unlock()\n\1"),
           lambda s: s + "\nvar mutantMu sync.Mutex\n",
           sub(r'import \(\n', 'import (\n\t"sync"\n'))),
    # Open re-takes the store flock for its read (the pre-t_1f4b978a Open):
    # every request queues on the flock again.
    ("open_takes_flock", "session.go",
     sub(r"(\t// No flock: save replaces map\.json by atomic rename, so this read sees one\n\t// whole map \(t_1f4b978a\)\.\n)",
         r"\1\tif lk, err := s.lock(); err == nil {\n\t\tdefer unlock(lk)\n\t}\n")),
    # Tool-schema walk: re-validate every declared schema per tool (O(T * bytes)).
    ("tool_walk_quadratic", "request.go",
     sub(r"(\t\t\tschemaRaw := bytes\.Clone\(raw\[schemaNode\.start:schemaNode\.end\]\)\n)",
         r"\1\t\t\tfor _, other := range tools.items {\n\t\t\t\t_ = json.Valid(raw[other.start:other.end])\n\t\t\t}\n")),
    # Symbol allocation: linear collision scan over the whole symbol table.
    ("symbol_alloc_linear_scan", "session.go",
     sub(r"(\talias := symbol\(st\.Binding, kind, original\)\n)",
         r'\1\tfor a, e := range st.Symbols {\n\t\tif a != alias && symbol(st.Binding, e.Kind, e.Original) == alias {\n\t\t\treturn "", Error("symbol_collision")\n\t\t}\n\t}\n')),
    # Response JSON decode: the t_129cf1ac shape (scan the remainder at every byte).
    ("decode_json_unanchored_scan", "prose.go",
     sub(r"(func \(m \*RequestMap\) decodeText\(text string\) \(string, error\) \{\n)",
         r"\1\tfor i := range text {\n\t\t_ = strings.IndexByte(text[i:], 0)\n\t}\n")),
    # Response SSE decode: rescan the whole stream history on every Feed.
    ("decode_sse_history_rescan", "stream.go",
     chain(sub(r"(\ts\.buffer = append\(s\.buffer, raw\.\.\.\)\n)",
               r"\1\tmutantSeen = append(mutantSeen, raw...)\n\t_ = bytes.IndexByte(mutantSeen, 0)\n"),
           lambda s: s + "\nvar mutantSeen []byte\n")),
]


def run_gates():
    p = subprocess.run(["go", "test", "./" + str(PKG), "-run", "^(" + "|".join(GATES) + ")$",
                       "-count=1", "-v"], capture_output=True, text=True)
    out = p.stdout + p.stderr
    status = {g: ("RED" if f"--- FAIL: {g}" in out else "green" if f"--- PASS: {g}" in out else "no-run") for g in GATES}
    return status, out


def main():
    base, out = run_gates()
    if any(v != "green" for v in base.values()):
        print(out)
        sys.exit(f"baseline not green: {base}")
    rows, gaps = [], []
    for name, fname, mutate in MUTANTS:
        path = PKG / fname
        src = path.read_text()
        try:
            path.write_text(mutate(src))
            status, out = run_gates()
        finally:
            subprocess.run(["git", "checkout", "--", str(path)], check=True)
        if "build failed" in out or "[setup failed]" in out:
            print(out)
            sys.exit(f"mutant {name} did not compile")
        caught = [g for g, v in status.items() if v == "RED"]
        rows.append((name, status))
        if not caught:
            gaps.append(name)
    w = max(len(n) for n, _ in rows)
    print(f"{'mutant':<{w}}  " + "  ".join(g.replace("TestPrepare", "")[:24] for g in GATES))
    for name, status in rows:
        print(f"{name:<{w}}  " + "  ".join(f"{status[g]:<24}" for g in GATES))
    if gaps:
        sys.exit(f"uncaught mutants: {gaps}")


if __name__ == "__main__":
    main()
