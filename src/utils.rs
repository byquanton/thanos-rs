use std::fs;
use std::path::PathBuf;

pub fn copy_except_region(input_dir: &str, output_dir: &str) -> std::io::Result<()> {
    let mut dirs_to_process = vec![PathBuf::from(input_dir)];
    while let Some(current_dir) = dirs_to_process.pop() {
        let directory = fs::read_dir(&current_dir).map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!(
                    "Failed to read directory '{}': {}",
                    current_dir.display(),
                    err
                ),
            )
        })?;
        for entry in directory {
            let entry = entry.map_err(| err| {
                std::io::Error::new(
                    err.kind(),
                    format!(
                        "Failed to access directory entry in '{}': {}",
                        current_dir.display(),
                        err
                    ),
                )
            })?;
            let path = entry.path();

            if path.is_dir() {
                if path.ends_with("region") {
                    continue;
                }

                dirs_to_process.push(path.clone());
                let relative_path = path.strip_prefix(input_dir).unwrap();
                let destination = PathBuf::from(output_dir).join(relative_path);
                fs::create_dir_all(&destination).map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!(
                            "Failed to create directory '{}': {}",
                            destination.display(),
                            err
                        ),
                    )
                })?;
            } else {
                let relative_path = path.strip_prefix(input_dir).unwrap();
                let destination = PathBuf::from(output_dir).join(relative_path);
                fs::copy(&path, &destination).map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!(
                            "Failed to copy file '{}' to '{}': {}",
                            path.display(),
                            destination.display(),
                            err
                        ),
                    )
                })?;
            }
        }
    }

    Ok(())
}