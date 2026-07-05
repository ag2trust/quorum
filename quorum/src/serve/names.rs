//! Name pool: load agent names from a file, acquire/release with >2*cap assertion.
//! When no file is given (or the file pool is exhausted), names are auto-generated
//! from a built-in word list + random suffix (e.g. `Anvil-x3k9`).

use std::collections::HashSet;
use std::path::Path;

const WORD_LIST: &[&str] = &[
    "Anvil", "Beacon", "Cleat", "Drift", "Ember", "Flint", "Glyph", "Hinge", "Ingot", "Joist",
    "Keel", "Lever", "Mortar", "Notch", "Optic", "Pivot", "Quill", "Rivet", "Spool", "Tread",
    "Umbra", "Valve", "Wedge", "Yoke", "Zenith", "Alloy", "Bolt", "Crank", "Dowel", "Facet",
    "Gauge", "Hasp",
];

pub struct Pool {
    available: Vec<String>,
    in_use: HashSet<String>,
    has_file: bool,
}

#[derive(Debug, PartialEq)]
pub enum AcquireResult {
    FromPool(String),
    Generated(String),
}

impl AcquireResult {
    pub fn name(&self) -> &str {
        match self {
            Self::FromPool(n) | Self::Generated(n) => n,
        }
    }

    pub fn into_name(self) -> String {
        match self {
            Self::FromPool(n) | Self::Generated(n) => n,
        }
    }

    pub fn is_generated(&self) -> bool {
        matches!(self, Self::Generated(_))
    }
}

impl Pool {
    pub fn new_generated() -> Self {
        Self {
            available: Vec::new(),
            in_use: HashSet::new(),
            has_file: false,
        }
    }

    pub fn load(path: &Path, cap: usize) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read names file {}: {e}", path.display()))?;

        let names: Vec<String> = content
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();

        if names.is_empty() {
            return Err(format!(
                "names file {} is empty (no usable names after filtering comments/blanks)",
                path.display()
            ));
        }

        let required = 2 * cap + 1;
        if names.len() < required {
            let deficit = required - names.len();
            eprintln!(
                "warn: names file has {} names, need {} for cap={}; {} names will be auto-generated on demand",
                names.len(),
                required,
                cap,
                deficit,
            );
        }

        Ok(Self {
            available: names,
            in_use: HashSet::new(),
            has_file: true,
        })
    }

    pub fn acquire(&mut self) -> AcquireResult {
        if let Some(name) = self.acquire_from_pool() {
            return AcquireResult::FromPool(name);
        }
        AcquireResult::Generated(self.generate())
    }

    fn acquire_from_pool(&mut self) -> Option<String> {
        let idx = self
            .available
            .iter()
            .position(|n| !self.in_use.contains(n))?;
        let name = self.available.remove(idx);
        self.in_use.insert(name.clone());
        Some(name)
    }

    fn generate(&mut self) -> String {
        for _ in 0..100 {
            let word = WORD_LIST[self.simple_random() % WORD_LIST.len()];
            let suffix = self.random_suffix();
            let name = format!("{word}-{suffix}");
            if !self.in_use.contains(&name) {
                self.in_use.insert(name.clone());
                return name;
            }
        }
        unreachable!("100 generate attempts all collided");
    }

    fn simple_random(&self) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        self.in_use.len().hash(&mut h);
        h.finish() as usize
    }

    fn random_suffix(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut h);
        std::thread::current().id().hash(&mut h);
        self.in_use.len().hash(&mut h);
        self.available.len().hash(&mut h);
        let val = h.finish();
        let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789".chars().collect();
        (0..4)
            .map(|i| chars[((val >> (i * 8)) as usize) % chars.len()])
            .collect()
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
        } else {
            self.in_use.insert(name.to_string());
        }
        true
    }

    pub fn has_file(&self) -> bool {
        self.has_file
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

        let result = pool.acquire();
        assert!(!result.is_generated());
        assert!(names.contains(&result.name()));
        assert_eq!(pool.in_use_count(), 1);

        pool.release(result.name());
        assert_eq!(pool.in_use_count(), 0);
    }

    #[test]
    fn too_few_names_warns_but_succeeds() {
        let (_f, path) = write_names_file(&["A", "B", "C"]);
        let pool = Pool::load(&path, 4).unwrap();
        assert!(pool.has_file());
        assert_eq!(pool.available.len(), 3);
    }

    #[test]
    fn empty_names_file_errors() {
        let (_f, path) = write_names_file(&["# just a comment", "", "  "]);
        let result = Pool::load(&path, 4);
        let err = result.err().expect("should fail for empty file");
        assert!(err.contains("empty"), "error was: {err}");
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
            let r = pool.acquire();
            assert!(!r.is_generated());
        }
        let generated = pool.acquire();
        assert!(generated.is_generated());
        assert!(generated.name().contains('-'));
        assert_eq!(pool.in_use_count(), 11);
    }

    #[test]
    fn generated_only_pool() {
        let mut pool = Pool::new_generated();
        assert!(!pool.has_file());

        let n1 = pool.acquire();
        let n2 = pool.acquire();
        assert!(n1.is_generated());
        assert!(n2.is_generated());
        assert_ne!(n1.name(), n2.name());
        assert!(n1.name().contains('-'));
        assert_eq!(pool.in_use_count(), 2);

        pool.release(n1.name());
        assert_eq!(pool.in_use_count(), 1);
    }

    #[test]
    fn reclaim_generated_name() {
        let mut pool = Pool::new_generated();
        assert!(pool.reclaim("Anvil-x3k9"));
        assert_eq!(pool.in_use_count(), 1);
    }

    #[test]
    fn reclaim_moves_name_to_in_use() {
        let names: Vec<&str> = vec!["A", "B", "C", "D", "E", "F", "G", "H", "I"];
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();

        assert!(pool.reclaim("C"));
        assert_eq!(pool.in_use_count(), 1);

        let mut acquired = Vec::new();
        for _ in 0..8 {
            let n = pool.acquire();
            acquired.push(n.into_name());
        }
        assert!(!acquired.contains(&"C".to_string()));

        pool.release("C");
        assert_eq!(pool.in_use_count(), 8);
    }

    #[test]
    fn reclaim_unknown_name_tracks_it() {
        let names: Vec<&str> = vec!["A", "B", "C", "D", "E", "F", "G", "H", "I"];
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();
        assert!(pool.reclaim("Unknown"));
        assert_eq!(pool.in_use_count(), 1);
    }

    #[test]
    fn reclaim_already_in_use_returns_true() {
        let names: Vec<&str> = vec!["A", "B", "C", "D", "E", "F", "G", "H", "I"];
        let (_f, path) = write_names_file(&names);
        let mut pool = Pool::load(&path, 4).unwrap();
        pool.acquire();
        let first = pool.acquire().into_name();
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
    fn generated_names_are_unique() {
        let mut pool = Pool::new_generated();
        let mut names = HashSet::new();
        for _ in 0..50 {
            let n = pool.acquire().into_name();
            assert!(names.insert(n.clone()), "duplicate generated name: {n}");
        }
    }

    #[test]
    fn startup_check_only_when_file_given() {
        // No file: generated-only pool starts fine
        let pool = Pool::new_generated();
        assert!(!pool.has_file());

        // Small file: starts and supplements with generated names
        let (_f, path) = write_names_file(&["Alpha", "Beta", "Gamma"]);
        let mut pool = Pool::load(&path, 4).unwrap();
        assert!(pool.has_file());
        // Exhaust the file names, then get generated ones
        for _ in 0..3 {
            let r = pool.acquire();
            assert!(!r.is_generated());
        }
        let gen = pool.acquire();
        assert!(gen.is_generated());
    }
}
