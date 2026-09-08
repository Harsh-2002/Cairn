//! Privileged process-kill validation of the production io_uring staging adapter.
//! This freezes an isolated ext4 filesystem, not the host filesystem, and is not a power-loss test.
//! Run the compiled test executable as root with `--ignored --nocapture`; it creates its own
//! private mount namespace, one 64-MiB image/loop device, and removes them before returning.

#![cfg(all(target_os = "linux", feature = "io-uring"))]
#![forbid(unsafe_code)]

use cairn_blob::{LocalBlobStore, open_lock_file_nofollow, try_lock_exclusive};
use cairn_types::storage::io::{StorageIoLease, StorageIoWatch};
use cairn_types::storage::{
    PlannedStorageWrite, StorageAdmission, StorageCleanup, StorageIntentPath, StoragePathRole,
    StorageToken, StorageWritePlan, StorageWriteTarget,
};
use cairn_types::{
    BodyStream, BucketName, ObjectKey, StageOptions, StoragePath, Timestamp, VersionId,
    traits::BlobStore,
};
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const TEST: &str = "real_uring_write_stays_owned_after_sigkill_until_kernel_teardown";
const ROLE: &str = "CAIRN_TEST_URING_DEATH_ROLE";
const ROOT: &str = "CAIRN_TEST_URING_DEATH_ROOT";
const DEADLINE: Duration = Duration::from_secs(10);

fn command(program: &str, args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new("timeout")
        .args(["--kill-after=2s", "10s", program])
        .args(args)
        .output()
        .unwrap()
}

fn checked(program: &str, args: &[&std::ffi::OsStr]) -> std::process::Output {
    let result = command(program, args);
    assert!(
        result.status.success(),
        "{program} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    result
}

/// Cleanup also runs during assertion unwinding. The watchdog independently thaws this exact
/// mount if a kernel observation or recovery call unexpectedly blocks the controller.
struct Fixture {
    root: tempfile::TempDir,
    mounted: bool,
    loop_device: Option<String>,
    writer: Option<Child>,
    watchdog: Option<(mpsc::Sender<()>, std::thread::JoinHandle<bool>)>,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("cairn-uring-process-death-")
            .tempdir_in("/SSD/dev")
            .unwrap();
        let mut fixture = Self {
            root,
            mounted: false,
            loop_device: None,
            writer: None,
            watchdog: None,
        };
        let image = fixture.root.path().join("filesystem.img");
        File::create_new(&image)
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        checked(
            "mkfs.ext4",
            &[
                "-q".as_ref(),
                "-F".as_ref(),
                "-E".as_ref(),
                "lazy_itable_init=0,lazy_journal_init=0".as_ref(),
                image.as_os_str(),
            ],
        );
        let device = checked(
            "losetup",
            &["--find".as_ref(), "--show".as_ref(), image.as_os_str()],
        );
        let device = String::from_utf8(device.stdout).unwrap().trim().to_owned();
        assert!(device.starts_with("/dev/loop"));
        fixture.loop_device = Some(device.clone());
        std::fs::create_dir(fixture.mount()).unwrap();
        checked(
            "mount",
            &[
                "-t".as_ref(),
                "ext4".as_ref(),
                "-o".as_ref(),
                "noatime,nodiscard".as_ref(),
                device.as_ref(),
                fixture.mount().as_os_str(),
            ],
        );
        fixture.mounted = true;
        println!(
            "owned loop device: {device}; fixture: {}",
            fixture.root.path().display()
        );
        fixture
    }

    fn mount(&self) -> PathBuf {
        self.root.path().join("mount")
    }

    fn freeze(&mut self) {
        let mount = self.mount();
        let (stop, stopped) = mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            if stopped.recv_timeout(Duration::from_secs(30)).is_ok() {
                return false;
            }
            // This mount was created by this fixture, inside this process's private namespace.
            let _ = command("fsfreeze", &["--unfreeze".as_ref(), mount.as_os_str()]);
            true
        });
        self.watchdog = Some((stop, watchdog));
        checked("fsfreeze", &["--freeze".as_ref(), self.mount().as_os_str()]);
    }

    fn thaw(&mut self) {
        checked(
            "fsfreeze",
            &["--unfreeze".as_ref(), self.mount().as_os_str()],
        );
        if let Some((stop, watchdog)) = self.watchdog.take() {
            let _ = stop.send(());
            assert!(
                !watchdog.join().unwrap(),
                "filesystem thaw watchdog expired"
            );
        }
    }

    fn reap(&mut self) {
        let deadline = Instant::now() + DEADLINE;
        let writer = self.writer.as_mut().unwrap();
        loop {
            if let Some(status) = writer.try_wait().unwrap() {
                assert_eq!(
                    status.signal(),
                    Some(9),
                    "writer was not terminated by SIGKILL"
                );
                self.writer = None;
                return;
            }
            assert!(
                Instant::now() < deadline,
                "killed writer failed to exit after thaw"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn finish(&mut self) {
        assert!(self.writer.is_none());
        checked("umount", &[self.mount().as_os_str()]);
        self.mounted = false;
        let device = self.loop_device.as_ref().unwrap();
        checked("losetup", &["--detach".as_ref(), device.as_ref()]);
        self.loop_device = None;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(self.root.path().join("writer.log")) {
                eprintln!("writer output:\n{log}");
            }
            if let Some(writer) = &self.writer {
                if let Some(ring) = ring(writer.id()) {
                    eprintln!("last ring observation:\n{}", ring.evidence);
                }
                if let Ok(tasks) = std::fs::read_dir(format!("/proc/{}/task", writer.id())) {
                    for task in tasks.flatten().take(16) {
                        if let Ok(stack) = std::fs::read_to_string(task.path().join("stack")) {
                            eprintln!("task {}:\n{stack}", task.file_name().to_string_lossy());
                        }
                    }
                }
            }
        }
        if self.mounted {
            let _ = command(
                "fsfreeze",
                &["--unfreeze".as_ref(), self.mount().as_os_str()],
            );
        }
        if let Some((stop, watchdog)) = self.watchdog.take() {
            let _ = stop.send(());
            let _ = watchdog.join();
        }
        if let Some(writer) = self.writer.as_mut() {
            let _ = writer.kill();
            let deadline = Instant::now() + DEADLINE;
            while writer.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        if self.mounted {
            let _ = command("umount", &[self.mount().as_os_str()]);
        }
        if let Some(device) = &self.loop_device {
            let _ = command("losetup", &["--detach".as_ref(), device.as_ref()]);
        }
    }
}

fn writer() {
    let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
    let node = open_lock_file_nofollow(&root.join("node.lock")).unwrap();
    try_lock_exclusive(&node).unwrap();
    let planned = PlannedStorageWrite::new(
        BucketName::parse("bucket").unwrap(),
        StorageToken::generate(),
        StorageWriteTarget::Object {
            key: ObjectKey::parse("key").unwrap(),
            version_id: VersionId::null(),
            row_id: StorageToken::generate().as_str().to_owned(),
        },
    )
    .unwrap();
    let plan = planned.plan().clone();
    let node = Arc::new(node);
    let (_, initialization_lease) = StorageIoWatch::new(
        StorageToken::generate(),
        plan.generation.clone(),
        node.clone(),
    );
    let (_, lease) = StorageIoWatch::new(plan.attempt.clone(), plan.generation.clone(), node);
    let permit = planned
        .admit(StorageAdmission::Granted(Box::new(plan.clone())), lease)
        .unwrap();
    let runtime = runtime();
    runtime.block_on(async {
        let store = LocalBlobStore::open(root.join("mount/store"), initialization_lease)
            .await
            .unwrap()
            .with_io_uring(true);
        let body: BodyStream = Box::pin(futures_util::stream::once(async move {
            let StorageWriteTarget::Object { row_id, .. } = &plan.target else {
                unreachable!()
            };
            let manifest = format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n",
                plan.attempt.as_str(),
                plan.generation.as_str(),
                row_id,
                plan.paths[0].path,
                plan.paths[1].path,
                plan.paths[2].path
            );
            // Staging has already created and flocked the exact file, and the production ring
            // executor is ready. This barrier controls timing only; kernel evidence is required
            // separately before the controller is allowed to send SIGKILL.
            std::fs::write(root.join("ready"), manifest).unwrap();
            tokio::task::spawn_blocking(|| std::io::stdin().read_exact(&mut [0u8; 1]))
                .await
                .unwrap()
                .unwrap();
            Ok(bytes::Bytes::from(vec![0x39; 256 * 1024]))
        }));
        store
            .stage(
                permit,
                body,
                StageOptions {
                    size_ceiling: 1024 * 1024,
                    ..StageOptions::default()
                },
            )
            .await
            .unwrap();
    });
    panic!("writer must be killed while its first real ring write is frozen");
}

fn plan_from_ready(path: &Path) -> StorageWritePlan {
    let manifest = std::fs::read_to_string(path).unwrap();
    let fields: Vec<_> = manifest.lines().collect();
    assert_eq!(fields.len(), 6);
    let plan = StorageWritePlan {
        attempt: StorageToken::try_from(fields[0].to_owned()).unwrap(),
        generation: StorageToken::try_from(fields[1].to_owned()).unwrap(),
        bucket: BucketName::parse("bucket").unwrap(),
        target: StorageWriteTarget::Object {
            key: ObjectKey::parse("key").unwrap(),
            version_id: VersionId::null(),
            row_id: fields[2].to_owned(),
        },
        paths: [
            StoragePathRole::Temporary,
            StoragePathRole::Final,
            StoragePathRole::IndexSpool,
        ]
        .into_iter()
        .zip(&fields[3..])
        .map(|(role, path)| StorageIntentPath {
            role,
            path: StoragePath::from_string((*path).to_owned()),
        })
        .collect(),
    };
    plan.validate().unwrap();
    plan
}

#[derive(Debug)]
struct Ring {
    submitted: u64,
    completed: u64,
    evidence: String,
}

fn ring(pid: u32) -> Option<Ring> {
    for entry in std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.flatten() {
        if std::fs::read_link(entry.path())
            .is_ok_and(|target| target.to_string_lossy().contains("io_uring"))
        {
            let evidence = std::fs::read_to_string(format!(
                "/proc/{pid}/fdinfo/{}",
                entry.file_name().to_string_lossy()
            ))
            .ok()?;
            let field = |name| {
                evidence
                    .lines()
                    .find_map(|line| line.strip_prefix(name)?.trim().parse::<u64>().ok())
            };
            return Some(Ring {
                submitted: field("CachedSqHead:")?,
                completed: field("CqTail:")?,
                evidence,
            });
        }
    }
    None
}

fn frozen_ring_stack(pid: u32) -> Option<String> {
    for entry in std::fs::read_dir(format!("/proc/{pid}/task"))
        .ok()?
        .flatten()
    {
        let Ok(stack) = std::fs::read_to_string(entry.path().join("stack")) else {
            continue;
        };
        if stack.contains("io_write") && stack.contains("percpu_rwsem_wait") {
            return Some(stack);
        }
    }
    None
}

fn wait_for(label: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + DEADLINE;
    while !predicate() {
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn lease(attempt: &StorageToken, generation: &StorageToken) -> StorageIoLease {
    StorageIoWatch::new(attempt.clone(), generation.clone(), Arc::new(())).1
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
#[ignore = "requires root, private mount namespaces, ext4 loop mounts and readable kernel stacks"]
fn real_uring_write_stays_owned_after_sigkill_until_kernel_teardown() {
    match std::env::var(ROLE).as_deref() {
        Ok("writer") => return writer(),
        Ok("namespace") => {}
        _ => {
            let status = Command::new("unshare")
                .args(["--mount", "--propagation", "private"])
                .arg(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", TEST, "--nocapture"])
                .env(ROLE, "namespace")
                .status()
                .unwrap();
            assert!(
                status.success(),
                "isolated io_uring process-kill regression failed"
            );
            return;
        }
    }
    let mut fixture = Fixture::new();
    let runtime = runtime();
    let store = runtime
        .block_on(LocalBlobStore::open(
            fixture.mount().join("store"),
            cairn_types::testing::fixture_storage_io(),
        ))
        .unwrap();
    let writer_log = File::create_new(fixture.root.path().join("writer.log")).unwrap();
    let writer = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", TEST, "--nocapture"])
        .env(ROLE, "writer")
        .env(ROOT, fixture.root.path())
        .env("CAIRN_URING_THREADS", "1")
        .stdin(Stdio::piped())
        .stdout(writer_log.try_clone().unwrap())
        .stderr(writer_log)
        .spawn()
        .unwrap();
    let pid = writer.id();
    fixture.writer = Some(writer);
    let ready = fixture.root.path().join("ready");
    wait_for("production staging body barrier", || ready.exists());
    let plan = plan_from_ready(&ready);
    let path = fixture
        .mount()
        .join("store")
        .join(plan.paths[0].path.as_str());
    assert!(path.exists());
    let initial = ring(pid).expect("production io_uring descriptor missing");
    let node = open_lock_file_nofollow(&fixture.root.path().join("node.lock")).unwrap();
    assert_eq!(
        try_lock_exclusive(&node).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    fixture.freeze();
    fixture
        .writer
        .as_mut()
        .unwrap()
        .stdin
        .take()
        .unwrap()
        .write_all(&[1])
        .unwrap();
    wait_for(
        "kernel-consumed pending ring write in filesystem freeze",
        || {
            ring(pid).is_some_and(|current| {
                current.submitted > initial.submitted && current.submitted > current.completed
            }) && frozen_ring_stack(pid).is_some()
        },
    );
    println!(
        "actual pending kernel ring:\n{}",
        ring(pid).unwrap().evidence
    );
    println!(
        "kernel freeze witness:\n{}",
        frozen_ring_stack(pid).unwrap()
    );
    fixture.writer.as_mut().unwrap().kill().unwrap();
    // A pending SIGKILL is not claimed to be a completed process exit. On kernels that drain
    // io_uring before exit_files, the node lock itself must remain held during this interval.
    assert!(
        frozen_ring_stack(pid).is_some(),
        "no post-SIGKILL kernel-I/O interval observed"
    );
    assert!(
        fixture
            .writer
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        try_lock_exclusive(&node).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let file_probe = File::open(&path).unwrap();
    assert_eq!(
        try_lock_exclusive(&file_probe).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(file_probe);
    println!(
        "SIGKILL is pending; real kernel write, node exclusion and file flock remain outstanding until thaw"
    );
    let cleanup = StorageCleanup {
        id: StorageToken::generate(),
        bucket: plan.bucket.clone(),
        path: plan.paths[0].path.clone(),
        quota_debt_id: None,
        claim_token: StorageToken::generate(),
        generation: StorageToken::generate(),
        lease_until: Timestamp(1000),
    };
    runtime.block_on(async {
        assert!(
            store
                .confirm_storage_quiescence(&plan, lease(&plan.attempt, &plan.generation))
                .await
                .is_err()
        );
        assert!(
            store
                .cleanup_storage(&cleanup, lease(&cleanup.id, &cleanup.generation))
                .await
                .is_err()
        );
    });
    assert!(path.exists(), "cleanup removed a kernel-owned staging file");
    fixture.thaw();
    fixture.reap();
    try_lock_exclusive(&node).unwrap();
    runtime.block_on(async {
        store
            .confirm_storage_quiescence(&plan, lease(&plan.attempt, &plan.generation))
            .await
            .unwrap();
        for intent_path in &plan.paths {
            let cleanup = StorageCleanup {
                path: intent_path.path.clone(),
                ..cleanup.clone()
            };
            store
                .cleanup_storage(&cleanup, lease(&cleanup.id, &cleanup.generation))
                .await
                .unwrap();
        }
    });
    assert!(!path.exists());
    assert!(
        !fixture
            .mount()
            .join("store")
            .join(plan.final_path().unwrap().as_str())
            .exists()
    );
    drop(node);
    drop(store);
    drop(runtime);
    fixture.finish();
}
