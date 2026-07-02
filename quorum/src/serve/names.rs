//! Name pool: load agent names from a file, acquire/release with >2*cap assertion.

use std::collections::HashSet;
use std::path::Path;

pub struct Pool {
    available: Vec<String>,
    in_use: HashSet<String>,
}

impl Pool {
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
        })
    }

    pub fn acquire(&mut self) -> Option<String> {
        let idx = self
            .available
            .iter()
            .position(|n| !self.in_use.contains(n))?;
        let name = self.available.remove(idx);
        self.in_use.insert(name.clone());
        Some(name)
    }

    pub fn release(&mut self, name: &str) {
        if self.in_use.remove(name) {
            self.available.push(name.to_string());
        }
    }

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

        let n1 = pool.acquire().unwrap();
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
    fn acquire_exhaustion() {
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
            assert!(pool.acquire().is_some());
        }
        assert!(pool.acquire().is_none());
    }

    #[test]
    fn comments_and_blanks_skipped() {
        let (_f, path) =
            write_names_file(&["# comment", "A", "", "B", "C", "D", "E", "F", "G", "H", "I"]);
        let pool = Pool::load(&path, 4).unwrap();
        assert_eq!(pool.available.len(), 9);
    }
}
