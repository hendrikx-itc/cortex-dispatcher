use std::os::unix::fs::symlink;
use std::fs::{hard_link, copy, Permissions, set_permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use futures::stream::Stream;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::event::FileEvent;
use crate::{settings, settings::LocalTargetMethod};

pub fn to_stream(
    settings: &settings::DirectoryTarget,
    receiver: UnboundedReceiver<FileEvent>,
) -> impl futures::Stream<Item = FileEvent, Error = ()> {
    let overwrite = settings.overwrite;
    let target_name = settings.name.clone();
    let target_directory = settings.directory.clone();
    let method = settings.method.clone();
    let target_permissions = Permissions::from_mode(settings.permissions);

    receiver.map_err(|e| {
        error!("[E01006] Error receiving: {}", e);
    }).map(move |file_event: FileEvent| -> FileEvent {
        let source_path_str = file_event.path.to_str().unwrap();
        let file_name = file_event.path.file_name().unwrap();
        let target_path = target_directory.join(file_name);
        let target_path_str = target_path.to_str().unwrap();
        let target_perms = target_permissions.clone();

        debug!("FileEvent for {}: '{}'", &target_name, &source_path_str);

        if overwrite {
            // If overwrite is enabled, we just always try to remove the target and
            // expect that a NotFound error might be returned.
            let remove_result = std::fs::remove_file(&target_path);

            match remove_result {
                Ok(_) => debug!("Removed existing target '{}'", &target_path_str),
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            // Ok, this can happen
                        },
                        _ => {
                            // Unexpected error, so log it
                            error!("Error removing file '{}': {}", &target_path_str, e);
                        }
                    }
                }
            }
        }

        match method {
            LocalTargetMethod::Copy => {
                let result = copy(&file_event.path, &target_path);

                match result {
                    Ok(size) => {
                        debug!("'{}' copied {} bytes to '{}'", &source_path_str, size, &target_path_str);
                    }
                    Err(e) => {
                        error!(
                            "[E01005] Error copying '{}' to '{}': {}",
                            &source_path_str, &target_path_str, &e
                        );
                    }
                }
            },
            LocalTargetMethod::Hardlink => {
                let result = hard_link(&file_event.path, &target_path);

                match result {
                    Ok(()) => {
                        debug!("Hardlinked '{}' to '{}'", &source_path_str, &target_path_str);
                    }
                    Err(e) => {
                        error!(
                            "[E01004] Error hardlinking '{}' to '{}': {}",
                            &source_path_str, &target_path_str, &e
                        );
                    }
                }
            },
            LocalTargetMethod::Symlink => {
                let result = symlink(&file_event.path, &target_path);

                match result {
                    Ok(()) => {
                        debug!("Symlinked '{}' to '{}'", &source_path_str, &target_path_str);
                    }
                    Err(e) => {
                        error!(
                            "[E01007] Error symlinking '{}' to '{}': {}",
                            &source_path_str, &target_path_str, &e
                        );
                    }
                }
            }
        }

        set_permissions(&target_path, target_perms);

        FileEvent {
            source_name: target_name.clone(),
            path: PathBuf::from(target_path)
        }
    })
}
