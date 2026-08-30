//! Live host-resource telemetry for status and daemon diagnostics.
//!
//! Sampling is deliberately outside `quorum-core`: it performs platform I/O,
//! is never persisted, and must run only after status has closed its SQLite
//! read connection. The daemon runs the same sampler in a detached,
//! single-flight worker so a stuck platform syscall cannot stall its tick.

use quorum_core::stats::{
    DiskResourceView, HealthVerdict, HostResourcesView, MemoryResourceView, ResourceSeverity, Stats,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const DEFAULT_RESOURCE_POLL_SECS: u64 = 30;
pub const DEFAULT_DISK_WARN_FREE_GIB: u64 = 80;
pub const DEFAULT_DISK_CRITICAL_FREE_GIB: u64 = 40;
pub const DEFAULT_MEMORY_WARN_AVAILABLE_PCT: u8 = 15;
pub const DEFAULT_MEMORY_CRITICAL_AVAILABLE_PCT: u8 = 8;
pub const MIN_RESOURCE_POLL_SECS: u64 = 5;
pub const MAX_RESOURCE_POLL_SECS: u64 = 3600;
const SAMPLE_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RESOURCE_SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceMonitorConfig {
    pub poll_secs: u64,
    pub disk_warn_free_gib: u64,
    pub disk_critical_free_gib: u64,
    pub memory_warn_available_pct: u8,
    pub memory_critical_available_pct: u8,
}

impl Default for ResourceMonitorConfig {
    fn default() -> Self {
        Self {
            poll_secs: DEFAULT_RESOURCE_POLL_SECS,
            disk_warn_free_gib: DEFAULT_DISK_WARN_FREE_GIB,
            disk_critical_free_gib: DEFAULT_DISK_CRITICAL_FREE_GIB,
            memory_warn_available_pct: DEFAULT_MEMORY_WARN_AVAILABLE_PCT,
            memory_critical_available_pct: DEFAULT_MEMORY_CRITICAL_AVAILABLE_PCT,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiskTarget {
    pub label: &'static str,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct MemoryReading {
    total_bytes: u64,
    available_bytes: u64,
    swap_total_bytes: u64,
    swap_used_bytes: u64,
}

#[derive(Debug, Clone)]
struct DiskReading {
    device_id: u64,
    sampled_path: PathBuf,
    total_bytes: u64,
    available_bytes: u64,
}

/// Test seam around platform syscalls. Production sampling never shells out.
trait PlatformSampler {
    fn memory(&self) -> std::io::Result<MemoryReading>;
    fn disk(&self, path: &Path) -> std::io::Result<DiskReading>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemSampler;

impl PlatformSampler for SystemSampler {
    fn memory(&self) -> std::io::Result<MemoryReading> {
        platform_memory()
    }

    fn disk(&self, path: &Path) -> std::io::Result<DiskReading> {
        platform_disk(path)
    }
}

pub fn sample_system(
    sampled_at: i64,
    targets: &[DiskTarget],
    config: ResourceMonitorConfig,
) -> HostResourcesView {
    sample_with(&SystemSampler, sampled_at, targets, config)
}

#[derive(Debug)]
struct InFlightSample {
    started_at: Instant,
    sampled_at: i64,
    receiver: std::sync::mpsc::Receiver<HostResourcesView>,
    timeout_reported: bool,
}

/// Keeps daemon sampling off the async executor and limits it to one detached
/// platform call at a time. A detached thread is intentional: Tokio waits for
/// blocking-pool jobs during runtime shutdown, while a filesystem syscall may
/// not return promptly on a degraded mount.
#[derive(Debug)]
pub struct ResourceSamplePoller {
    last_started: Option<Instant>,
    in_flight: Option<InFlightSample>,
    timeout: Duration,
}

impl Default for ResourceSamplePoller {
    fn default() -> Self {
        Self {
            last_started: None,
            in_flight: None,
            timeout: RESOURCE_SAMPLE_TIMEOUT,
        }
    }
}

impl ResourceSamplePoller {
    /// Poll an existing sample or start one when due. This never waits for the
    /// sampler and never starts a replacement while one remains in flight.
    pub fn poll(
        &mut self,
        now: Instant,
        sampled_at: i64,
        targets: &[DiskTarget],
        config: ResourceMonitorConfig,
    ) -> Option<HostResourcesView> {
        if let Some(in_flight) = self.in_flight.as_mut() {
            match in_flight.receiver.try_recv() {
                Ok(resources) => {
                    self.in_flight = None;
                    return Some(resources);
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    let sampled_at = in_flight.sampled_at;
                    self.in_flight = None;
                    return Some(failed_sample(sampled_at, "sampler worker stopped"));
                }
                Err(std::sync::mpsc::TryRecvError::Empty)
                    if !in_flight.timeout_reported
                        && now.saturating_duration_since(in_flight.started_at) >= self.timeout =>
                {
                    in_flight.timeout_reported = true;
                    return Some(failed_sample(
                        in_flight.sampled_at,
                        format!(
                            "sampler exceeded {}s; awaiting the existing sample",
                            self.timeout.as_secs()
                        ),
                    ));
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => return None,
            }
        }

        let due = self.last_started.is_none_or(|last| {
            now.saturating_duration_since(last) >= Duration::from_secs(config.poll_secs)
        });
        if !due {
            return None;
        }

        self.last_started = Some(now);
        let (sender, receiver) = std::sync::mpsc::channel();
        let targets = targets.to_vec();
        match std::thread::Builder::new()
            .name("quorum-resource-sampler".to_string())
            .spawn(move || {
                let _ = sender.send(sample_system(sampled_at, &targets, config));
            }) {
            Ok(handle) => {
                drop(handle);
                self.in_flight = Some(InFlightSample {
                    started_at: now,
                    sampled_at,
                    receiver,
                    timeout_reported: false,
                });
                None
            }
            Err(error) => Some(failed_sample(
                sampled_at,
                format!("sampler worker start: {error}"),
            )),
        }
    }
}

fn failed_sample(sampled_at: i64, error: impl Into<String>) -> HostResourcesView {
    HostResourcesView {
        sampled_at,
        complete: false,
        severity: ResourceSeverity::Normal,
        memory: None,
        disks: Vec::new(),
        errors: vec![error.into()],
    }
}

fn sample_with(
    sampler: &dyn PlatformSampler,
    sampled_at: i64,
    targets: &[DiskTarget],
    config: ResourceMonitorConfig,
) -> HostResourcesView {
    let mut errors = Vec::new();
    let memory = match sampler.memory() {
        Ok(reading) => Some(memory_view(reading, config)),
        Err(error) => {
            errors.push(format!("memory: {error}"));
            None
        }
    };

    // Device ID, rather than textual path, is the filesystem identity. This
    // avoids presenting duplicate capacity for a DB and worktree base on the
    // same volume.
    let mut disks_by_device: BTreeMap<u64, DiskResourceView> = BTreeMap::new();
    for target in targets {
        match sampler.disk(&target.path) {
            Ok(reading) => {
                if let Some(existing) = disks_by_device.get_mut(&reading.device_id) {
                    if !existing.targets.iter().any(|label| label == target.label) {
                        existing.targets.push(target.label.to_string());
                    }
                    continue;
                }
                let available_percent = percent(reading.available_bytes, reading.total_bytes);
                disks_by_device.insert(
                    reading.device_id,
                    DiskResourceView {
                        targets: vec![target.label.to_string()],
                        path: reading.sampled_path.to_string_lossy().into_owned(),
                        total_bytes: reading.total_bytes,
                        available_bytes: reading.available_bytes,
                        available_percent,
                        severity: disk_severity(reading.available_bytes, config),
                    },
                );
            }
            Err(error) => errors.push(format!("{} filesystem: {error}", target.label)),
        }
    }

    let disks: Vec<_> = disks_by_device.into_values().collect();
    let severity = memory
        .iter()
        .map(|value| value.severity)
        .chain(disks.iter().map(|value| value.severity))
        .max()
        .unwrap_or_default();
    HostResourcesView {
        sampled_at,
        complete: errors.is_empty(),
        severity,
        memory,
        disks,
        errors,
    }
}

fn memory_view(reading: MemoryReading, config: ResourceMonitorConfig) -> MemoryResourceView {
    let available_percent = percent(reading.available_bytes, reading.total_bytes);
    MemoryResourceView {
        total_bytes: reading.total_bytes,
        available_bytes: reading.available_bytes,
        available_percent,
        swap_total_bytes: reading.swap_total_bytes,
        swap_used_bytes: reading.swap_used_bytes,
        severity: memory_severity(available_percent, config),
    }
}

fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 * 100.0 / total as f64).clamp(0.0, 100.0)
    }
}

fn memory_severity(available_percent: f64, config: ResourceMonitorConfig) -> ResourceSeverity {
    if available_percent <= f64::from(config.memory_critical_available_pct) {
        ResourceSeverity::Critical
    } else if available_percent <= f64::from(config.memory_warn_available_pct) {
        ResourceSeverity::Warning
    } else {
        ResourceSeverity::Normal
    }
}

fn disk_severity(available_bytes: u64, config: ResourceMonitorConfig) -> ResourceSeverity {
    if available_bytes <= config.disk_critical_free_gib.saturating_mul(GIB) {
        ResourceSeverity::Critical
    } else if available_bytes <= config.disk_warn_free_gib.saturating_mul(GIB) {
        ResourceSeverity::Warning
    } else {
        ResourceSeverity::Normal
    }
}

/// Attach live telemetry and promote only a healthy status verdict. Resource
/// pressure must never conceal an existing stalled pipeline.
pub fn attach_to_stats(stats: &mut Stats, resources: HostResourcesView) {
    if resources.severity != ResourceSeverity::Normal && stats.health == HealthVerdict::OnTrack {
        stats.health = HealthVerdict::Attention;
    }
    stats.resources = Some(resources);
}

#[derive(Debug, Default)]
pub struct ResourceTransitionMonitor {
    last_severity: Option<ResourceSeverity>,
    last_failure_log: Option<Instant>,
}

impl ResourceTransitionMonitor {
    /// Return transition/failure lines to log. An incomplete sample is
    /// fail-open: it can report a rate-limited diagnostic, but cannot prove a
    /// severity change or recovery.
    pub fn observe(&mut self, resources: &HostResourcesView, now: Instant) -> Vec<String> {
        let mut lines = Vec::new();
        if !resources.complete
            && self
                .last_failure_log
                .is_none_or(|last| now.duration_since(last) >= SAMPLE_ERROR_LOG_INTERVAL)
        {
            self.last_failure_log = Some(now);
            lines.push(format!(
                "RESOURCE SAMPLE WARNING (fail-open): {}",
                resources.errors.join("; ")
            ));
        }
        if !resources.complete {
            return lines;
        }

        let current = resources.severity;
        let previous = self.last_severity.replace(current);
        if previous == Some(current) || (previous.is_none() && current == ResourceSeverity::Normal)
        {
            return lines;
        }
        let summary = resource_summary(resources);
        let line = match (previous, current) {
            (
                Some(ResourceSeverity::Warning | ResourceSeverity::Critical),
                ResourceSeverity::Normal,
            ) => {
                format!("RESOURCE RECOVERED: {summary}")
            }
            (Some(ResourceSeverity::Critical), ResourceSeverity::Warning) => {
                format!("RESOURCE WARNING (recovered from critical): {summary}")
            }
            (_, ResourceSeverity::Warning) => format!("RESOURCE WARNING: {summary}"),
            (_, ResourceSeverity::Critical) => format!("RESOURCE CRITICAL: {summary}"),
            (_, ResourceSeverity::Normal) => return lines,
        };
        lines.push(line);
        lines
    }
}

pub fn resource_summary(resources: &HostResourcesView) -> String {
    let mut parts = Vec::new();
    if let Some(memory) = &resources.memory {
        parts.push(format!(
            "memory {:.1}/{:.1} GiB available ({:.0}%), swap {:.1}/{:.1} GiB used",
            bytes_to_gib(memory.available_bytes),
            bytes_to_gib(memory.total_bytes),
            memory.available_percent,
            bytes_to_gib(memory.swap_used_bytes),
            bytes_to_gib(memory.swap_total_bytes),
        ));
    }
    for disk in &resources.disks {
        parts.push(format!(
            "disk[{}] {:.1}/{:.1} GiB available ({:.0}%) at {}",
            disk.targets.join("+"),
            bytes_to_gib(disk.available_bytes),
            bytes_to_gib(disk.total_bytes),
            disk.available_percent,
            disk.path,
        ));
    }
    parts.join("; ")
}

pub fn bytes_to_gib(bytes: u64) -> f64 {
    bytes as f64 / GIB as f64
}

fn existing_path(path: &Path) -> std::io::Result<PathBuf> {
    let mut candidate = path.to_path_buf();
    loop {
        if candidate.exists() {
            return candidate.canonicalize().or(Ok(candidate));
        }
        if !candidate.pop() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no existing ancestor for {}", path.display()),
            ));
        }
    }
}

#[cfg(unix)]
fn platform_disk(path: &Path) -> std::io::Result<DiskReading> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let sampled_path = existing_path(path)?;
    let c_path = std::ffi::CString::new(sampled_path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("filesystem path contains NUL: {}", sampled_path.display()),
        )
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = stats.f_frsize;
    let metadata = std::fs::metadata(&sampled_path)?;
    Ok(DiskReading {
        device_id: metadata.dev(),
        sampled_path,
        total_bytes: filesystem_bytes(stats.f_blocks, block_size),
        available_bytes: filesystem_bytes(stats.f_bavail, block_size),
    })
}

#[cfg(unix)]
fn filesystem_bytes(blocks: libc::fsblkcnt_t, block_size: libc::c_ulong) -> u64 {
    u128::from(blocks)
        .saturating_mul(u128::from(block_size))
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(not(unix))]
fn platform_disk(path: &Path) -> std::io::Result<DiskReading> {
    let _ = path;
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "filesystem telemetry is unsupported on this platform",
    ))
}

#[cfg(target_os = "macos")]
#[allow(deprecated)] // libc marks Mach host calls deprecated in favor of a new dependency.
fn platform_memory() -> std::io::Result<MemoryReading> {
    let total_bytes: u64 = macos_sysctl("hw.memsize")?;
    let swap: libc::xsw_usage = macos_sysctl("vm.swapusage")?;
    let mut vm = std::mem::MaybeUninit::<libc::vm_statistics64>::zeroed();
    let mut count = libc::HOST_VM_INFO64_COUNT;
    let result = unsafe {
        libc::host_statistics64(
            libc::mach_host_self(),
            libc::HOST_VM_INFO64,
            vm.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS {
        return Err(std::io::Error::other(format!(
            "host_statistics64 failed with kern_return_t {result}"
        )));
    }
    let vm = unsafe { vm.assume_init() };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    // Inactive and speculative pages are reclaimable by the VM. Clamp to
    // physical memory because platform counters are sampled independently.
    let available_pages = u64::from(vm.free_count)
        .saturating_add(u64::from(vm.inactive_count))
        .saturating_add(u64::from(vm.speculative_count));
    let available_bytes = available_pages
        .saturating_mul(page_size as u64)
        .min(total_bytes);
    Ok(MemoryReading {
        total_bytes,
        available_bytes,
        swap_total_bytes: swap.xsu_total,
        swap_used_bytes: swap.xsu_used,
    })
}

#[cfg(target_os = "macos")]
fn macos_sysctl<T>(name: &str) -> std::io::Result<T> {
    let name = std::ffi::CString::new(name).expect("static sysctl names contain no NUL");
    let mut value = std::mem::MaybeUninit::<T>::zeroed();
    let mut size = std::mem::size_of::<T>();
    if unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    if size != std::mem::size_of::<T>() {
        return Err(std::io::Error::other(format!(
            "sysctl returned {size} bytes, expected {}",
            std::mem::size_of::<T>()
        )));
    }
    Ok(unsafe { value.assume_init() })
}

#[cfg(target_os = "linux")]
fn platform_memory() -> std::io::Result<MemoryReading> {
    let source = std::fs::read_to_string("/proc/meminfo")?;
    let mut values = BTreeMap::new();
    for line in source.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        if let Some(kib) = rest
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<u64>().ok())
        {
            values.insert(key, kib.saturating_mul(1024));
        }
    }
    let get = |key: &str| {
        values.get(key).copied().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("/proc/meminfo lacks {key}"),
            )
        })
    };
    let total_bytes = get("MemTotal")?;
    let available_bytes = values.get("MemAvailable").copied().unwrap_or_else(|| {
        ["MemFree", "Buffers", "Cached"]
            .iter()
            .filter_map(|key| values.get(*key))
            .copied()
            .fold(0, u64::saturating_add)
    });
    let swap_total_bytes = values.get("SwapTotal").copied().unwrap_or(0);
    let swap_free_bytes = values.get("SwapFree").copied().unwrap_or(0);
    Ok(MemoryReading {
        total_bytes,
        available_bytes: available_bytes.min(total_bytes),
        swap_total_bytes,
        swap_used_bytes: swap_total_bytes.saturating_sub(swap_free_bytes),
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn platform_memory() -> std::io::Result<MemoryReading> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "memory telemetry is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct FakeSampler {
        memory: std::io::Result<MemoryReading>,
        disks: BTreeMap<PathBuf, std::io::Result<DiskReading>>,
        disk_calls: Cell<usize>,
    }

    impl PlatformSampler for FakeSampler {
        fn memory(&self) -> std::io::Result<MemoryReading> {
            self.memory
                .as_ref()
                .copied()
                .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
        }

        fn disk(&self, path: &Path) -> std::io::Result<DiskReading> {
            self.disk_calls.set(self.disk_calls.get() + 1);
            self.disks
                .get(path)
                .expect("fake disk path")
                .as_ref()
                .cloned()
                .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
        }
    }

    fn gib(value: u64) -> u64 {
        value * GIB
    }

    fn memory(available_gib: u64) -> MemoryReading {
        MemoryReading {
            total_bytes: gib(100),
            available_bytes: gib(available_gib),
            swap_total_bytes: gib(8),
            swap_used_bytes: gib(2),
        }
    }

    fn disk(device_id: u64, path: &str, available_gib: u64) -> DiskReading {
        DiskReading {
            device_id,
            sampled_path: PathBuf::from(path),
            total_bytes: gib(500),
            available_bytes: gib(available_gib),
        }
    }

    fn fake(memory_reading: MemoryReading, disk_readings: &[(&str, DiskReading)]) -> FakeSampler {
        FakeSampler {
            memory: Ok(memory_reading),
            disks: disk_readings
                .iter()
                .map(|(path, reading)| (PathBuf::from(path), Ok(reading.clone())))
                .collect(),
            disk_calls: Cell::new(0),
        }
    }

    fn targets() -> Vec<DiskTarget> {
        vec![
            DiskTarget {
                label: "database",
                path: PathBuf::from("/db"),
            },
            DiskTarget {
                label: "worktrees",
                path: PathBuf::from("/worktrees"),
            },
        ]
    }

    #[test]
    fn classifies_resources_and_deduplicates_same_filesystem() {
        let sampler = fake(
            memory(12),
            &[
                ("/db", disk(7, "/db", 35)),
                ("/worktrees", disk(7, "/worktrees", 35)),
            ],
        );
        let view = sample_with(&sampler, 123, &targets(), ResourceMonitorConfig::default());

        assert!(view.complete);
        assert_eq!(view.severity, ResourceSeverity::Critical);
        assert_eq!(
            view.memory.as_ref().unwrap().severity,
            ResourceSeverity::Warning
        );
        assert_eq!(view.disks.len(), 1);
        assert_eq!(view.disks[0].targets, ["database", "worktrees"]);
        assert_eq!(view.disks[0].severity, ResourceSeverity::Critical);
        assert_eq!(sampler.disk_calls.get(), 2);
    }

    #[test]
    fn json_shape_uses_raw_bytes_and_lowercase_severity() {
        let sampler = fake(
            memory(50),
            &[
                ("/db", disk(1, "/volume", 120)),
                ("/worktrees", disk(1, "/volume", 120)),
            ],
        );
        let view = sample_with(&sampler, 456, &targets(), ResourceMonitorConfig::default());
        let mut stats = Stats::default();
        attach_to_stats(&mut stats, view);
        let json = serde_json::to_value(&stats).unwrap();
        let json = &json["resources"];

        assert_eq!(json["sampled_at"], 456);
        assert_eq!(json["severity"], "normal");
        assert_eq!(json["memory"]["available_bytes"], gib(50));
        assert_eq!(json["memory"]["swap_used_bytes"], gib(2));
        assert_eq!(
            json["disks"][0]["targets"],
            serde_json::json!(["database", "worktrees"])
        );
    }

    #[test]
    fn resource_pressure_promotes_only_on_track_health() {
        let sampler = fake(
            memory(7),
            &[
                ("/db", disk(1, "/volume", 120)),
                ("/worktrees", disk(1, "/volume", 120)),
            ],
        );
        let resources = sample_with(&sampler, 1, &targets(), ResourceMonitorConfig::default());
        let mut healthy = Stats::default();
        attach_to_stats(&mut healthy, resources.clone());
        assert_eq!(healthy.health, HealthVerdict::Attention);

        let mut stalled = Stats {
            health: HealthVerdict::Stalled,
            ..Stats::default()
        };
        attach_to_stats(&mut stalled, resources);
        assert_eq!(stalled.health, HealthVerdict::Stalled);
    }

    #[test]
    fn transition_logging_deduplicates_and_reports_recovery() {
        let warning_sampler = fake(
            memory(12),
            &[
                ("/db", disk(1, "/volume", 120)),
                ("/worktrees", disk(1, "/volume", 120)),
            ],
        );
        let normal_sampler = fake(
            memory(50),
            &[
                ("/db", disk(1, "/volume", 120)),
                ("/worktrees", disk(1, "/volume", 120)),
            ],
        );
        let critical_sampler = fake(
            memory(7),
            &[
                ("/db", disk(1, "/volume", 120)),
                ("/worktrees", disk(1, "/volume", 120)),
            ],
        );
        let warning = sample_with(
            &warning_sampler,
            1,
            &targets(),
            ResourceMonitorConfig::default(),
        );
        let normal = sample_with(
            &normal_sampler,
            2,
            &targets(),
            ResourceMonitorConfig::default(),
        );
        let critical = sample_with(
            &critical_sampler,
            3,
            &targets(),
            ResourceMonitorConfig::default(),
        );
        let start = Instant::now();
        let mut monitor = ResourceTransitionMonitor::default();

        let first = monitor.observe(&warning, start);
        assert_eq!(first.len(), 1);
        assert!(first[0].starts_with("RESOURCE WARNING:"));
        assert!(monitor
            .observe(&warning, start + Duration::from_secs(30))
            .is_empty());
        let escalated = monitor.observe(&critical, start + Duration::from_secs(60));
        assert_eq!(escalated.len(), 1);
        assert!(escalated[0].starts_with("RESOURCE CRITICAL:"));
        assert!(monitor
            .observe(&critical, start + Duration::from_secs(90))
            .is_empty());
        let recovered = monitor.observe(&normal, start + Duration::from_secs(120));
        assert_eq!(recovered.len(), 1);
        assert!(recovered[0].starts_with("RESOURCE RECOVERED:"));
        assert!(monitor
            .observe(&normal, start + Duration::from_secs(150))
            .is_empty());
    }

    #[test]
    fn incomplete_samples_are_fail_open_and_error_logs_are_rate_limited() {
        let sampler = FakeSampler {
            memory: Err(std::io::Error::other("unavailable")),
            disks: [
                (PathBuf::from("/db"), Ok(disk(1, "/volume", 120))),
                (PathBuf::from("/worktrees"), Ok(disk(1, "/volume", 120))),
            ]
            .into_iter()
            .collect(),
            disk_calls: Cell::new(0),
        };
        let incomplete = sample_with(&sampler, 1, &targets(), ResourceMonitorConfig::default());
        let start = Instant::now();
        let mut monitor = ResourceTransitionMonitor::default();

        assert!(!incomplete.complete);
        assert_eq!(monitor.observe(&incomplete, start).len(), 1);
        assert!(monitor
            .observe(&incomplete, start + Duration::from_secs(60))
            .is_empty());
        assert_eq!(
            monitor
                .observe(&incomplete, start + SAMPLE_ERROR_LOG_INTERVAL)
                .len(),
            1
        );
    }

    #[test]
    fn poller_times_out_once_without_replacing_the_in_flight_sample() {
        let start = Instant::now();
        let timeout = Duration::from_secs(2);
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut poller = ResourceSamplePoller {
            last_started: Some(start),
            in_flight: Some(InFlightSample {
                started_at: start,
                sampled_at: 123,
                receiver,
                timeout_reported: false,
            }),
            timeout,
        };

        let timed_out = poller
            .poll(
                start + timeout,
                456,
                &targets(),
                ResourceMonitorConfig::default(),
            )
            .expect("deadline should produce one incomplete sample");
        assert!(!timed_out.complete);
        assert_eq!(timed_out.sampled_at, 123);
        assert!(timed_out.errors[0].contains("sampler exceeded 2s"));
        assert!(poller.in_flight.is_some());

        assert!(
            poller
                .poll(
                    start + timeout + Duration::from_secs(1),
                    789,
                    &targets(),
                    ResourceMonitorConfig::default(),
                )
                .is_none(),
            "the same stalled sample must not produce repeated warnings or replacements"
        );
        assert!(poller.in_flight.is_some());

        sender
            .send(HostResourcesView {
                sampled_at: 123,
                complete: true,
                severity: ResourceSeverity::Normal,
                memory: None,
                disks: Vec::new(),
                errors: Vec::new(),
            })
            .unwrap();
        let completed = poller
            .poll(
                start + timeout + Duration::from_secs(2),
                999,
                &targets(),
                ResourceMonitorConfig::default(),
            )
            .expect("completed in-flight sample should be collected");
        assert!(completed.complete);
        assert!(poller.in_flight.is_none());
    }
}
