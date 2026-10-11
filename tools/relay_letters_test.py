"""A relay that remembers 702 player names must keep admitting joiners.

The lobby hands out origin letters a..z, then aa..zz (702 in all). A relay keeps
every name's letter in relay_letters.json across restarts, so a long-running
public relay eventually remembers all 702; the allocator then looped forever
(the sequence wraps back to "aa" at index 702) and the relay stopped answering.
Now the oldest names that are not connected give their letters back. The
relay's two state files are also written atomically, so a crash mid-write
cannot make the next start forget every player's letter.

Unit checks of the allocator and the writer, then the real run_host in relay
mode on loopback with a relay_letters.json that already holds 702 names.

    python tools/relay_letters_test.py
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time
from unittest import mock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "netpunch"))

import lobby                                                  # noqa: E402
from punch import _pack, _unpack, TYPE_DATA, open_socket      # noqa: E402

lobby.BULK_TCP[0] = False
fails = []


def check(name, cond, extra=""):
    print(("ok   " if cond else "FAIL ") + name + (f"  ({extra})" if extra else ""))
    if not cond:
        fails.append(name)


# ---- the letter sequence itself is unchanged (the menu DLL's originName() matches it)
seq = [lobby._origin_letter(i) for i in range(lobby.ORIGIN_LETTER_COUNT)]
check("702 letters, all distinct", len(set(seq)) == 702 == lobby.ORIGIN_LETTER_COUNT)
check("a..z, then aa, ab ... zz",
      seq[0] == "a" and seq[25] == "z" and seq[26] == "aa" and seq[27] == "ab" and seq[701] == "zz")

# ---- _allocate_origin_letter
alloc = lobby._allocate_origin_letter
letters = {}
check("first free letter is a", alloc(letters)[0] == "a")
check("a reserved letter is skipped", alloc({}, reserved=("a",))[0] == "b")
check("holes are filled lowest first", alloc({"x": "a", "y": "c"})[0] == "b")

full = {f"old{i}": seq[i] for i in range(702)}
letter, released = alloc(dict(full))
check("all 702 taken, none connected: the oldest name's letter is reused", (letter, released) == ("a", ["old0"]))
letter, released = alloc(dict(full), connected={"old0", "old1"})
check("...connected names keep theirs: the oldest absent one goes", (letter, released) == ("c", ["old2"]))
copy = dict(full)
alloc(copy, connected={"old0"})
check("...and the released name is dropped from the map", "old1" not in copy and len(copy) == 701)
letter, released = alloc(dict(full), connected=set(full))
check("every letter held by a connected player: None, not a hang", letter is None and released == [])

# ---- _write_json_atomic
d = tempfile.mkdtemp(prefix="relayletters-")
p = os.path.join(d, "state.json")
check("writes a new file", lobby._write_json_atomic(p, {"a": 1}) and json.load(open(p)) == {"a": 1})
check("replaces an existing file", lobby._write_json_atomic(p, {"b": 2}) and json.load(open(p)) == {"b": 2})
check("leaves no temp file", not os.path.exists(p + ".tmp"))


def half_written(obj, f):
    f.write('{"c": ')
    raise OSError("disk full")


with mock.patch.object(lobby.json, "dump", half_written):
    ok = lobby._write_json_atomic(p, {"c": 3})
check("a write that fails half way reports failure", ok is False)
check("...keeps the previous file whole", json.load(open(p)) == {"b": 2})
check("...and cleans up its temp file", not os.path.exists(p + ".tmp"))

with mock.patch.object(lobby.os, "replace", side_effect=PermissionError("refused")):
    ok = lobby._write_json_atomic(p, {"d": 4})
check("a filesystem that refuses the rename still gets the write", ok is True and json.load(open(p)) == {"d": 4})
check("...without a stray temp file", not os.path.exists(p + ".tmp"))
check("an unwritable directory reports failure",
      lobby._write_json_atomic(os.path.join(d, "missing", "state.json"), {}) is False)


# ---- the real relay, with 702 remembered names
class Relay:
    def __init__(self, iodir, logs):
        self.io = lobby.LobbyIO(iodir)
        self.sock = open_socket(0, socket.AF_INET)
        self.port = self.sock.getsockname()[1]
        self.stop = threading.Event()
        self.thread = threading.Thread(
            target=lambda: lobby.run_host(self.sock, "relay", self.io, code="X", stop=self.stop,
                                          log=logs.append, relay_only=True),
            daemon=True)
        self.thread.start()
        time.sleep(0.3)

    def close(self):
        self.stop.set()
        self.thread.join(5)


def joiner(relay, name):
    c = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    c.bind(("127.0.0.1", 0))
    c.settimeout(0.2)
    c.sendto(_pack(TYPE_DATA, json.dumps({"t": "join", "name": name, "version": lobby.LOBBY_VERSION}).encode()),
             ("127.0.0.1", relay.port))
    return c


def roster_letters(c, seconds=4.0):
    """The letters of the newest roster this joiner receives, or {} if none came."""
    seen = {}
    end = time.time() + seconds
    while time.time() < end:
        try:
            data, _ = c.recvfrom(65536)
        except socket.timeout:
            continue
        ptype, payload = _unpack(data)
        if ptype != TYPE_DATA:
            continue
        try:
            m = json.loads(payload.decode("utf-8"))
        except ValueError:
            continue
        if m.get("t") == "roster" and m.get("letters"):
            seen = m["letters"]
            break
    return seen


iodir = tempfile.mkdtemp(prefix="relayletters-io-")
letters_file = os.path.join(iodir, "relay_letters.json")
json.dump(full, open(letters_file, "w"))
logs = []
relay = Relay(iodir, logs)
c = joiner(relay, "newcomer")
got = roster_letters(c)
check("a joiner is admitted although the relay remembers 702 names", bool(got), "no roster: the relay hung" if not got else "")
check("it gets the oldest remembered name's letter", got.get("newcomer") == "a", str(got))
check("the log says letters were released", any("origin letters were taken" in s for s in logs))
saved = json.load(open(letters_file)) if os.path.exists(letters_file) else {}
check("the file keeps 702 names with the newcomer in and the oldest out",
      len(saved) == 702 and saved.get("newcomer") == "a" and "old0" not in saved, f"{len(saved)} names")
check("...with no temp file left", not os.path.exists(letters_file + ".tmp"))
relay.close()

# ---- an unreadable state file is reported, not silently forgotten
iodir = tempfile.mkdtemp(prefix="relayletters-io-")
open(os.path.join(iodir, "relay_letters.json"), "w").write('{"bob": "b", "tru')       # cut off mid-write
open(os.path.join(iodir, "relay_companies.json"), "w").write("not json")
logs = []
relay = Relay(iodir, logs)
check("a truncated relay_letters.json is logged", any("cannot read" in s and "relay_letters.json" in s for s in logs), str(logs))
check("a corrupt relay_companies.json is logged", any("cannot read" in s and "relay_companies.json" in s for s in logs), str(logs))
c = joiner(relay, "carl")
got = roster_letters(c)
check("the relay still works: the first joiner is a", got.get("carl") == "a", str(got))
relay.close()

print()
if fails:
    print(f"{len(fails)} FAILED")
    sys.exit(1)
print("PASS: origin letters never run out, and the relay's state files are written atomically")
