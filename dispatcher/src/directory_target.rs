use std::os::unix::fs::symlink;
use std::fs::{hard_link, copy, Permissions, set_permissions};
use std::os::unix::fs::PermissionsExt;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::stream::StreamExt;

use crate::event::FileEvent;
use crate::{settings, settings::LocalTargetMethod};

pub fn to_stream(
    settings: &settings::DirectoryTarget,
    receiver: UnboundedReceiver<FileEvent>,
) -> impl futures::Stream<Item=FileEvent> + std::marker::Unpin {
    let overwrite = settings.overwrite;
    let target_name = settings.name.clone();
    let target_directory = settings.directory.clone();
    let method = settings.method.clone();
    let target_permissions = Permissions::from_mode(settings.permissions);

    receiver.map(move |file_event: FileEvent| -> FileEvent {
        let source_path_str = file_event.path.to_string_lossy();
        let file_name = file_event.path.file_name().unwrap();
        let target_path = target_directory.join(file_name);
        let target_path_str = target_path.to_string_lossy();
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

        let placement_result = match method {
            LocalTargetMethod::Copy => {
                let result = copy(&file_event.path, &target_path);

                match result {
                    Ok(size) => {
                        debug!("'{}' copied {} bytes to '{}'", &source_path_str, size, &target_path_str);
                        Ok(())
                    }
                    Err(e) => {
                        if overwrite {
                            // When overwrite is enabled, this should not occur, because any existing file should first be removed
                            error!(
                                "[E01005] Error copying '{}' to '{}': {}",
                                &source_path_str, &target_path_str, &e
                            );
                            Err(())
                        } else {
                            Ok(())
                        }
                    }
                }
            },
            LocalTargetMethod::Hardlink => {
                let result = hard_link(&file_event.path, &target_path);

                match result {
                    Ok(()) => {
                        debug!("Hardlinked '{}' to '{}'", &source_path_str, &target_path_str);
                        Ok(())
                    }
                    Err(e) => {
                        debug!("Error hardlinking: {}", e);

                        if overwrite {
                            // When overwrite is enabled, this should not occur, because any existing file should first be removed
                            error!(
                                "[E01004] Error hardlinking '{}' to '{}': {}",
                                &source_path_str, &target_path_str, &e
                            );
                            Err(())
                        } else {
                            Ok(())
                        }
                    }
                }
            },
            LocalTargetMethod::Symlink => {
                let result = symlink(&file_event.path, &target_path);

                match result {
                    Ok(()) => {
                        debug!("Symlinked '{}' to '{}'", &source_path_str, &target_path_str);
                        Ok(())
                    }
                    Err(e) => {
                        if overwrite {
                            // When overwrite is enabled, this should not occur, because any existing file should first be removed
                            error!(
                                "[E01007] Error symlinking '{}' to '{}': {}",
                                &source_path_str, &target_path_str, &e
                            );
                            Err(())
                        } else {
                            Ok(())
                        }
                    }
                }
            }
        };

        if let Ok(_) = placement_result {
            let set_result = set_permissions(&target_path, target_perms);

            if let Err(e) = set_result {
                error!("Could not set file permissions on '{}': {}", &target_path_str, e)
            }
        }

        FileEvent {
            source_name: target_name.clone(),
            path: target_path
        }
    })
}
