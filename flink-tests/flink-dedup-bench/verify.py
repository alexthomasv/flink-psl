from collections import defaultdict

def parse_line(line: str):
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    t = s.split()
    if len(t) < 8:
        return None
    op = t[5]
    try:
        lba = int(t[3])
    except ValueError:
        return None
    h = t[-1]
    if not h:
        return None
    return op, lba, h

def compute_refcounts(path: str):
    lba_hash = {}
    refcount = defaultdict(int)
    stats = dict(total_lines=0, parsed=0, writes=0, deltas=0)

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            stats["total_lines"] += 1
            parsed = parse_line(line)
            if not parsed:
                continue
            stats["parsed"] += 1
            op, lba, h = parsed
            if op.upper() != "W":
                continue
            stats["writes"] += 1

            old = lba_hash.get(lba)
            if old is None:
                lba_hash[lba] = h
                refcount[h] += 1
                stats["deltas"] += 1
            elif old != h:
                refcount[old] -= 1
                if refcount[old] == 0:
                    del refcount[old]
                refcount[h] += 1
                lba_hash[lba] = h
                stats["deltas"] += 2

    refcount = {h: c for h, c in refcount.items() if c > 0}
    stats.update(unique_hashes=len(refcount), unique_lbas=len(lba_hash))
    return refcount, lba_hash, stats

def compute_deltas(path: str):
    lba_hash = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = parse_line(line)
            if not parsed:
                continue
            op, lba, h = parsed
            if op.upper() != "W":
                continue
            old = lba_hash.get(lba)
            if old is None:
                lba_hash[lba] = h
                yield (h, +1)
            elif old != h:
                yield (old, -1)
                yield (h, +1)
                lba_hash[lba] = h

# --- NEW: read Flink output, keeping ONLY the latest line per hash ---
def read_flink_latest(path: str):
    """
    Reads a file with lines like 'hash,count' (may contain multiple lines per hash).
    Keeps only the *latest* occurrence for each hash and returns {hash: count}.
    Ignores empty / comment lines.
    """
    latest = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            # allow 'FINAL COUNTS> (hash,count)' style debug lines too
            if ">" in s and "(" in s and "," in s and ")" in s:
                # e.g., "FINAL COUNTS> (abcd,2)"
                try:
                    inside = s[s.index("(")+1:s.rindex(")")]
                    h, c = inside.split(",", 1)
                    latest[h.strip()] = int(c.strip())
                except Exception:
                    continue
                continue
            # normal 'hash,count'
            parts = s.split(",", 1)
            if len(parts) != 2:
                continue
            h, c_str = parts[0].strip(), parts[1].strip()
            if not h:
                continue
            try:
                c = int(c_str)
            except ValueError:
                continue
            latest[h] = c  # overwrites earlier occurrences → keep latest
    return latest

if __name__ == "__main__":
    import argparse, sys
    from itertools import islice
    import tempfile

    p = argparse.ArgumentParser(description="Compute overwrite-aware dedup refcounts.")
    p.add_argument("path", help="blkparse-like text file (reference input)")
    p.add_argument("--head", type=int, default=0,
                   help="only process first N lines of reference input (0 = all)")
    p.add_argument("--deltas", action="store_true",
                   help="print the (+1/-1) journal instead of final counts")
    p.add_argument("--top", type=int, default=0,
                   help="print only top K hashes by refcount (0 = all)")
    p.add_argument("--compare", help="Flink output file (hash,count lines). "
                                     "If provided, latest line per hash is used and "
                                     "validated against the reference.")
    p.add_argument("--allow-extras", action="store_true",
                   help="When comparing, ignore hashes present only in Flink output.")
    args = p.parse_args()

    # Optional head on the reference input
    if args.head > 0:
        with open(args.path, "r", encoding="utf-8", errors="ignore") as f, \
             tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            for line in islice(f, args.head):
                tmp.write(line)
            tmp.flush()
            ref_path = tmp.name
    else:
        ref_path = args.path

    if args.deltas:
        for h, d in compute_deltas(ref_path):
            print(f"{h},{d}")
        sys.exit(0)

    ref, lba, stats = compute_refcounts(ref_path)

    if not args.compare:
        # Just print reference counts (sorted)
        items = sorted(ref.items(), key=lambda x: (-x[1], x[0]))
        if args.top > 0:
            items = items[:args.top]
        for h, c in items:
            print(f"{h},{c}")
        sys.exit(0)

    # --- Compare mode ---
    flink_latest = read_flink_latest(args.compare)

    missing = []    # in ref but not in flink
    mismatch = []   # in both but counts differ
    extras = []     # in flink but not in ref (reported unless --allow-extras)

    # check that all reference entries are present & correct
    for h, c in ref.items():
        fc = flink_latest.get(h)
        if fc is None:
            missing.append(h)
        elif fc != c:
            mismatch.append((h, c, fc))

    if not args.allow_extras:
        for h in flink_latest.keys():
            if h not in ref:
                extras.append(h)

    # Print a compact report
    ok = (not missing) and (not mismatch) and (args.allow_extras or not extras)
    if ok:
        print("OK: all reference entries present and correct.")
        print(f"ref_hashes={len(ref)} flink_hashes={len(flink_latest)} "
              f"unique_lbas={stats.get('unique_lbas', 0)}")
        sys.exit(0)

    # Problems found
    if missing:
        print(f"MISSING ({len(missing)}):")
        for h in missing[:20]:
            print(f"  {h}")
        if len(missing) > 20:
            print(f"  ... and {len(missing)-20} more")

    if mismatch:
        print(f"MISMATCH ({len(mismatch)}): expected vs found")
        for h, exp, got in mismatch[:20]:
            print(f"  {h}: {exp} != {got}")
        if len(mismatch) > 20:
            print(f"  ... and {len(mismatch)-20} more")

    if extras and not args.allow_extras:
        print(f"EXTRAS ({len(extras)}): present only in Flink output")
        for h in extras[:20]:
            print(f"  {h}")
        if len(extras) > 20:
            print(f"  ... and {len(extras)-20} more")

    # Non-zero exit for CI/scripts
    sys.exit(1)