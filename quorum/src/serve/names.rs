//! Name pool: load agent names from a file or auto-generate them.
//! Pool exhaustion falls back to generation so reviewer provisioning never starves.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

const WORD_LIST: &[&str] = &[
    "Anvil", "Beacon", "Bolt", "Cadence", "Chisel", "Cipher", "Crank", "Dynamo", "Ember", "Flint",
    "Forge", "Fulcrum", "Gadget", "Gear", "Glint", "Hammer", "Hatch", "Ingot", "Jolt", "Kernel",
    "Lantern", "Lever", "Loom", "Mortar", "Needle", "Nozzle", "Optic", "Piston", "Plumb", "Prism",
    "Pulley", "Quartz", "Ratchet", "Rivet", "Rotor", "Sable", "Shard", "Shuttle", "Signal",
    "Spark", "Spindle", "Sprocket", "Strut", "Tether", "Toggle", "Torque", "Valve", "Vector",
    "Wedge", "Winch",
];

pub struct Pool {
    available: Vec<String>,
    in_use: HashSet<String>,
    /// When true, `acquire` falls back to generation on exhaustion.
    fallback_generate: bool,
}

fn generate_suffix() -> String {
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let mixed = count.wrapping_mul(6364136223846793005).wrapping_add(nanos);
    format!("{:x}", mixed & 0xFFFF)
}

impl Pool {
    /// Load names from a file. Validates `>2*cap` count. On pool exhaustion at
    /// runtime, falls back to auto-generation with a log line.
    pub fn load(path: &Path, cap: usize) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read names file {}: {e}", path.display()))?;

        let names: Vec<String> = content
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();

        let required = 2 * cap + 1;
        if names.len() < required {
            return Err(format!(
                "names file has {} names, need >2*cap ({required}) for cap={cap}",
                names.len()
            ));
        }

        Ok(Self {
            available: names,
            in_use: HashSet::new(),
            fallback_generate: true,
        })
    }

    /// Create a pool with no file — all names are auto-generated.
    pub fn generated() -> Self {
        Self {
            available: Vec::new(),
            in_use: HashSet::new(),
            fallback_generate: true,
        }
    }

    /// Acquire a name from the pool. If the pool is exhausted and fallback is
    /// enabled, generates a unique name (word + random suffix). Returns the name
    /// and whether it was generated (true) or from pool (false).
    pub fn acquire(&mut self) -> Option<(String, bool)> {
        if let Some(idx) = self.available.iter().position(|n| !self.in_use.contains(n)) {
            let name = self.available.remove(idx);
            self.in_use.insert(name.clone());
            return Some((name, false));
        }

        if self.fallback_generate {
            let name = self.generate_unique();
            self.in_use.insert(name.clone());
            return Some((name, true));
        }

        None
    }

    pub fn release(&mut self, name: &str) {
        if self.in_use.remove(name) {
            self.available.push(name.to_string());
        }
    }

    pub fn reclaim(&mut self, name: &str) -> bool {
        if self.in_use.contains(name) {
            return true;
        }
        if let Some(idx) = self.available.iter().position(|n| n == name) {
            let name = self.available.remove(idx);
            self.in_use.insert(name);
            true
        } else {
            false
        }
    }

    fn generate_unique(&self) -> String {
        for _ in 0..100 {
            let word = WORD_LIST[COUNTER.load(Ordering::Relaxed) as usize % WORD_LIST.len()];
            let suffix = generate_suffix();
            let candidate = format!("{word}-{suffix}");
            if !self.in_use.contains(&candidate) {
                return candidate;
            }
        }
        let fallback = format!("Agent-{}", generate_suffix());
        fallback
    }

    #[cfg(test)]
    pub fn in_use_count(&self) -> usize {
        self.in_use.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_names_file(names: &[&str]) -> (tempfile::NamedTempFile, std::path::PathBuf) {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for n in names {
            writeln!(f, "{n}").unwrap();
        }
        let path = f.path().to_path_buf();
        (f, path)
    }

    #[test]
    fn load_and_acquire_release() {
        let names: Vec<&str> = (0..10)
            .map(|i| match i {
                0 => "Alpha",
                1 => "Beta",
                2 => "Gamma",
                3 => "Delta",
                4 => "Epsilon",
                5 => "Zeta",
                6 => "Eta",
                7 => "Theta",
                8 => "Iota",
                9 => "Kappa",
                _ => unreachable!(),
            })
            .collect();
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();

        let (n1, generated) = pool.acquire().unwrap();
        assert!(!generated);
        assert!(names.contains(&n1.as_str()));
        assert_eq!(pool.in_use_count(), 1);

        pool.release(&n1);
        assert_eq!(pool.in_use_count(), 0);
    }

    #[test]
    fn too_few_names_errors() {
        let (_f, path) = write_names_file(&["A", "B", "C"]);
        let result = Pool::load(&path, 4);
        assert!(result.is_err());
    }

    #[test]
    fn pool_exhaustion_falls_back_to_generation() {
        let names: Vec<&str> = (0..10)
            .map(|i| match i {
                0 => "A",
                1 => "B",
                2 => "C",
                3 => "D",
                4 => "E",
                5 => "F",
                6 => "G",
                7 => "H",
                8 => "I",
                9 => "J",
                _ => unreachable!(),
            })
            .collect();
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();

        for _ in 0..10 {
            let (_, generated) = pool.acquire().unwrap();
            assert!(!generated);
        }

        // 11th acquire should generate
        let (name, generated) = pool.acquire().unwrap();
        assert!(generated);
        assert!(
            name.contains('-'),
            "generated name should have word-suffix format: {name}"
        );
        assert_eq!(pool.in_use_count(), 11);
    }

    #[test]
    fn generated_pool_produces_unique_names() {
        let mut pool = Pool::generated();
        let mut seen = HashSet::new();
        for _ in 0..20 {
            let (name, generated) = pool.acquire().unwrap();
            assert!(generated);
            assert!(name.contains('-'));
            assert!(seen.insert(name.clone()), "duplicate name: {name}");
        }
        assert_eq!(pool.in_use_count(), 20);
    }

    #[test]
    fn reclaim_moves_name_to_in_use() {
        let names: Vec<&str> = vec!["A", "B", "C", "D", "E", "F", "G", "H", "I"];
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();

        assert!(pool.reclaim("C"));
        assert_eq!(pool.in_use_count(), 1);

        let acquired: Vec<String> = (0..8)
            .filter_map(|_| pool.acquire().map(|(n, _)| n))
            .collect();
        assert_eq!(acquired.len(), 8);
        assert!(!acquired.contains(&"C".to_string()));

        pool.release("C");
        assert_eq!(pool.in_use_count(), 8);
    }

    #[test]
    fn reclaim_unknown_name_returns_false() {
        let names: Vec<&str> = vec!["A", "B", "C", "D", "E", "F", "G", "H", "I"];
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();
        assert!(!pool.reclaim("Unknown"));
    }

    #[test]
    fn reclaim_already_in_use_returns_true() {
        let names: Vec<&str> = vec!["A", "B", "C", "D", "E", "F", "G", "H", "I"];
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();
        pool.acquire().unwrap();
        let (first, _) = pool.acquire().unwrap();
        assert!(pool.reclaim(&first));
        assert_eq!(pool.in_use_count(), 2);
    }

    #[test]
    fn comments_and_blanks_skipped() {
        let (_f, path) =
            write_names_file(&["# comment", "A", "", "B", "C", "D", "E", "F", "G", "H", "I"]);
        let pool = Pool::load(&path, 4).unwrap();
        assert_eq!(pool.available.len(), 9);
    }

    #[test]
    fn no_names_file_startup_check_skipped() {
        let pool = Pool::generated();
        assert_eq!(pool.in_use_count(), 0);
        assert!(pool.available.is_empty());
    }
}
