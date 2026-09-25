/* Diagnostic-only glibc interposer. It does not ship in Cairn. Compile with
 * cc -O2 -fPIC -shared -o sync_probe.so sync_probe.c -ldl, then preload it into
 * a dynamically linked benchmark server. The Python harness owns the mmap
 * file and rejects an empty capture (static binaries bypass LD_PRELOAD).
 *
 * Counters are process-wide syscall wall times, not exclusive time or a PUT
 * critical-path decomposition. No names, file descriptors or payloads leave
 * the process. The classifier's fstat is deliberately outside the timer.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct counter {
    uint64_t calls;
    uint64_t total_ns;
    uint64_t max_ns;
};

struct capture {
    char magic[8];
    struct counter fsync_root;
    struct counter fsync_staging;
    struct counter fsync_other_dir;
    struct counter fsync_other;
    struct counter fdatasync;
};

static struct capture *capture;
static int (*next_fsync)(int);
static int (*next_fdatasync)(int);
static const char *data_root;

static uint64_t elapsed_ns(struct timespec start, struct timespec end) {
    return (uint64_t)(end.tv_sec - start.tv_sec) * 1000000000ULL
        + (uint64_t)(end.tv_nsec - start.tv_nsec);
}

static void record(struct counter *counter, uint64_t elapsed) {
    if (counter == NULL) return;
    __atomic_fetch_add(&counter->calls, 1, __ATOMIC_RELAXED);
    __atomic_fetch_add(&counter->total_ns, elapsed, __ATOMIC_RELAXED);
    uint64_t old = __atomic_load_n(&counter->max_ns, __ATOMIC_RELAXED);
    while (old < elapsed && !__atomic_compare_exchange_n(
        &counter->max_ns, &old, elapsed, 1, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {}
}

__attribute__((constructor)) static void initialize_probe(void) {
    next_fsync = dlsym(RTLD_NEXT, "fsync");
    next_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    data_root = getenv("CAIRN_DATA_DIR");
    const char *path = getenv("SYNC_PROBE_FILE");
    if (path == NULL) return;
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0) return;
    struct stat stat;
    if (fstat(fd, &stat) == 0 && stat.st_size == (off_t)sizeof(struct capture)) {
        void *mapped = mmap(NULL, sizeof(struct capture), PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, 0);
        if (mapped != MAP_FAILED) {
            struct capture *candidate = mapped;
            if (memcmp(candidate->magic, "CSYNCv2", 8) == 0) {
                capture = candidate;
            } else {
                munmap(mapped, sizeof(struct capture));
            }
        }
    }
    close(fd);
}

int fsync(int fd) {
    if (next_fsync == NULL) next_fsync = dlsym(RTLD_NEXT, "fsync");
    struct stat stat;
    struct counter *counter = NULL;
    if (capture != NULL) {
        counter = &capture->fsync_other;
        if (fstat(fd, &stat) == 0 && S_ISDIR(stat.st_mode)) {
            char fd_link[64];
            char target[1024];
            snprintf(fd_link, sizeof(fd_link), "/proc/self/fd/%d", fd);
            ssize_t len = readlink(fd_link, target, sizeof(target) - 1);
            if (len >= 0) {
                target[len] = '\0';
                if (data_root != NULL && strcmp(target, data_root) == 0) {
                    counter = &capture->fsync_root;
                } else if (strstr(target, "/.staging") != NULL) {
                    counter = &capture->fsync_staging;
                } else {
                    counter = &capture->fsync_other_dir;
                }
            } else {
                counter = &capture->fsync_other_dir;
            }
        }
    }
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    int result = next_fsync(fd);
    clock_gettime(CLOCK_MONOTONIC, &end);
    record(counter, elapsed_ns(start, end));
    return result;
}

int fdatasync(int fd) {
    if (next_fdatasync == NULL) next_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    int result = next_fdatasync(fd);
    clock_gettime(CLOCK_MONOTONIC, &end);
    record(capture == NULL ? NULL : &capture->fdatasync, elapsed_ns(start, end));
    return result;
}
