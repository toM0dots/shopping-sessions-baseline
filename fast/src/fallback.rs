// Local integration: retry the complete dataset with this repository's C++ engine.
// This module runs only before worker startup, normally after a self-exec.
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn clear_partial_output(out: &Path) -> io::Result<()> {
    for name in ["user_daily", "campaign_daily"] {
        let dir = out.join(name);
        std::fs::create_dir_all(&dir)?;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_file()
                && path.extension().is_some_and(|ext| ext == "parquet")
            {
                std::fs::remove_file(path)?;
            }
        }
    }
    Ok(())
}

fn baseline_path() -> io::Result<PathBuf> {
    if let Some(path) = std::env::var_os("SS_BASELINE_BIN") {
        return Ok(path.into());
    }
    let exe = std::env::current_exe()?;
    let parent = exe.parent().ok_or_else(|| io::Error::other("executable has no parent directory"))?;
    Ok(parent.join("shopping-sessions"))
}

pub fn run(data: &str, out: &str, ncpu: usize, reason: &str) -> ! {
    let attempt = || -> io::Result<()> {
        let baseline = baseline_path()?;
        clear_partial_output(Path::new(out))?;
        if std::env::var("SS_ENGINE_TRACE").is_ok_and(|v| v == "1")
            || std::env::var("XL_TIMING").is_ok_and(|v| v == "1")
        {
            eprintln!("[engine] fallback: {reason}");
        }
        // Replace this process; no fast-path threads or buffered files survive.
        Err(Command::new(baseline)
            .env("DATA_DIR", data)
            .env("OUTPUT_DIR", out)
            .env("NCPU", ncpu.max(1).to_string())
            .exec())
    };
    let error = attempt().unwrap_err();
    eprintln!("[engine] fallback failed ({reason}): {error}");
    std::process::exit(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_removes_partial_parts_and_preserves_unrelated_files() {
        let dir = std::env::temp_dir().join(format!("shopping-fallback-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("user_daily")).unwrap();
        std::fs::create_dir_all(dir.join("campaign_daily")).unwrap();
        std::fs::write(dir.join("user_daily/old.parquet"), b"partial").unwrap();
        std::fs::write(dir.join("campaign_daily/old.parquet"), b"partial").unwrap();
        std::fs::write(dir.join("user_daily/_SUCCESS"), b"keep").unwrap();
        clear_partial_output(&dir).unwrap();
        assert!(!dir.join("user_daily/old.parquet").exists());
        assert!(!dir.join("campaign_daily/old.parquet").exists());
        assert_eq!(std::fs::read(dir.join("user_daily/_SUCCESS")).unwrap(), b"keep");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
