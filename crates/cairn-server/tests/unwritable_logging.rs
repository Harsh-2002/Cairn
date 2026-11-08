//! A failed log write must never take the server down.
//!
//! `tracing-subscriber`'s default internal-error path reports a failed writer with `eprintln!`.
//! If the process's stdout/stderr themselves are unwritable (a full filesystem, a closed pipe),
//! that fallback write fails and panics; with `panic = "abort"` the server dies with SIGABRT.
//! A storage server must keep serving when its log destination fills up, so the subscriber is
//! configured to drop internal write errors instead of reporting them on the same broken stream.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("local address").port()
}

fn healthy(port: u16) -> bool {
    let address = format!("127.0.0.1:{port}");
    let mut stream = match TcpStream::connect_timeout(
        &address.parse().expect("address"),
        Duration::from_millis(200),
    ) {
        Ok(stream) => stream,
        Err(_) => return false,
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout");
    if stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    stream.read_to_string(&mut response).is_ok() && response.starts_with("HTTP/1.1 200")
}

fn stop(mut child: Child) -> std::process::ExitStatus {
    let pid = child.id().to_string();
    let _ = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("send SIGTERM");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().expect("poll the server") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("server did not shut down within 30 seconds");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn unwritable_stdout_and_stderr_do_not_abort_the_server() {
    // `/dev/full` accepts opens and fails every write with ENOSPC, exactly like a full disk.
    let full = match std::fs::File::options().write(true).open("/dev/full") {
        Ok(file) => file,
        Err(error) => {
            eprintln!("skipping: /dev/full is unavailable: {error}");
            return;
        }
    };

    let port = free_port();
    let root = std::env::temp_dir().join(format!(
        "cairn-unwritable-logging-{}-{port}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create the test data directory");

    let child = Command::new(env!("CARGO_BIN_EXE_cairn"))
        .arg("serve")
        .env("CAIRN_DATA_DIR", root.join("data"))
        .env("CAIRN_DB_PATH", root.join("data/cairn.db"))
        .env("CAIRN_API_ADDR", format!("127.0.0.1:{port}"))
        .env("CAIRN_CONSOLE_ADDR", "off")
        .env("CAIRN_MASTER_KEY", "11".repeat(32))
        .env("CAIRN_LOG_LEVEL", "info")
        .stdout(Stdio::from(full.try_clone().expect("clone /dev/full")))
        .stderr(Stdio::from(full))
        .spawn()
        .expect("spawn the server");

    let deadline = Instant::now() + Duration::from_secs(20);
    while !healthy(port) {
        if Instant::now() >= deadline {
            let status = stop(child);
            let _ = std::fs::remove_dir_all(&root);
            panic!("server never became healthy; exit status: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let status = stop(child);
    let _ = std::fs::remove_dir_all(&root);
    assert!(
        status.success(),
        "unwritable logs must not abort the server: {status}"
    );
}
