"""A real child process proves the optional diagnostic interposer engages."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import sync_probe


class SyncProbeTests(unittest.TestCase):
    def test_capture_from_dynamic_child(self):
        compiler = shutil.which("cc")
        if not compiler or not sys.platform.startswith("linux"):
            self.skipTest("glibc compiler/probe platform unavailable")
        with tempfile.TemporaryDirectory(prefix="cairn-sync-probe-test-") as private:
            private = Path(private)
            library = private / "sync_probe.so"
            source = Path(__file__).with_name("sync_probe.c")
            subprocess.run([compiler, "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC",
                            "-shared", "-o", str(library), str(source), "-ldl"],
                           check=True, capture_output=True)
            capture = private / "capture.bin"
            sync_probe.initialize(capture)
            env = os.environ.copy()
            env["LD_PRELOAD"] = str(library)
            env["SYNC_PROBE_FILE"] = str(capture)
            env["CAIRN_DATA_DIR"] = str(private)
            child = ("import os,tempfile; "
                     "f=tempfile.TemporaryFile(); f.write(b'x'); f.flush(); "
                     "os.fdatasync(f.fileno()); os.fsync(f.fileno()); "
                     "d=os.open('.', os.O_RDONLY|os.O_DIRECTORY); os.fsync(d); os.close(d)")
            subprocess.run([sys.executable, "-c", child], cwd=private, env=env,
                           check=True, capture_output=True)
            result = sync_probe.read(capture)
            self.assertGreaterEqual(result["fdatasync"]["calls"], 1)
            self.assertGreaterEqual(result["fsync_other"]["calls"], 1)
            self.assertGreaterEqual(result["fsync_root"]["calls"], 1)
            self.assertGreaterEqual(result["fsync_root"]["total_ms"], 0)
            before = {name: {"calls": 1, "total_ms": 1.0, "max_ms": 1.0}
                      for name in sync_probe.FIELDS}
            after = {name: {"calls": 3, "total_ms": 4.0, "max_ms": 2.0}
                     for name in sync_probe.FIELDS}
            self.assertEqual(sync_probe.delta(before, after)["fsync_root"],
                             {"calls": 2, "total_ms": 3.0})

    def test_rejects_wrong_capture(self):
        with tempfile.TemporaryDirectory(prefix="cairn-sync-probe-test-") as private:
            capture = Path(private) / "capture.bin"
            sync_probe.initialize(capture)
            self.assertEqual(sync_probe.read(capture)["fsync_root"]["calls"], 0)
            capture.write_bytes(b"broken")
            with self.assertRaisesRegex(ValueError, "wrong size"):
                sync_probe.read(capture)


if __name__ == "__main__":
    unittest.main()
