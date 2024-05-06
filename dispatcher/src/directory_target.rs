use std::fs::{copy, hard_link, set_permissions, Permissions};
use std::os::unix::fs::symlink;
use std::os::unix::fs::PermissionsExt;

use log::{debug, error, warn};
use postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::Socket;

use crate::event::FileEvent;
use crate::persistence::PostgresAsyncPersistence;
use crate::{settings, settings::LocalTargetMethod};

pub async fn handle_file_event<T>(
    settings: &settings::DirectoryTarget,
    file_event: FileEvent,
    persistence: PostgresAsyncPersistence<T>,
) -> Result<FileEvent, String>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let overwrite = settings.overwrite;
    let target_name = settings.name.clone();
    let target_directory = settings.directory.clone();
    let method = settings.method.clone();
    let target_permissions = Permissions::from_mode(settings.permissions);

    let source_path_str = file_event.path.to_string_lossy();
    let file_name = match file_event.path.file_name() {
        Some(f) => f,
        None => {
            return Err(format!(
                "No file name from file event path '{}'",
                &source_path_str
            ));
        }
    };
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
                    }
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
                    debug!(
                        "'{}' copied {} bytes to '{}'",
                        &source_path_str, size, &target_path_str
                    );
                    Ok(())
                }
                Err(e) => {
                    if overwrite {
                        // When overwrite is enabled, this should not occur, because any existing
                        // file should first be removed
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
        }
        LocalTargetMethod::Hardlink => {
            let result = hard_link(&file_event.path, &target_path);

            match result {
                Ok(()) => {
                    debug!(
                        "Hardlinked '{}' to '{}'",
                        &source_path_str, &target_path_str
                    );
                    Ok(())
                }
                Err(e) => {
                    if overwrite {
                        // When overwrite is enabled, this should not occur, because any existing
                        // file should first be removed
                        error!(
                            "[E01004] Error hardlinking '{}' to '{}': {}",
                            &source_path_str, &target_path_str, &e
                        );
                        Err(())
                    } else {
                        // When overwrite is disabled, this might occur and is not an error, but it
                        // is reported
                        warn!(
                            "Could not hardlink '{}' to '{}': {}",
                            &source_path_str, &target_path_str, e
                        );
                        Ok(())
                    }
                }
            }
        }
        LocalTargetMethod::Symlink => {
            let result = symlink(&file_event.path, &target_path);

            match result {
                Ok(()) => {
                    debug!("Symlinked '{}' to '{}'", &source_path_str, &target_path_str);
                    Ok(())
                }
                Err(e) => {
                    if overwrite {
                        // When overwrite is enabled, this should not occur, because any existing
                        // file should first be removed
                        error!(
                            "[E01007] Error symlinking '{}' to '{}': {}",
                            &source_path_str, &target_path_str, &e
                        );
                        Err(())
                    } else {
                        // When overwrite is disabled, this might occur and is not an error, but it
                        // is reported
                        warn!(
                            "Could not symlink '{}' to '{}': {}",
                            &source_path_str, &target_path_str, e
                        );
                        Ok(())
                    }
                }
            }
        }
    };

    if placement_result.is_ok() {
        let set_result = set_permissions(&target_path, target_perms);

        if let Err(e) = set_result {
            error!(
                "Could not set file permissions on '{}': {}",
                &target_path_str, e
            )
        }
    }

    let insert_result = persistence
        .insert_dispatched(&target_name, file_event.file_id)
        .await;

    match insert_result {
        Ok(_) => debug!("Dispatched to directory"),
        Err(e) => debug!("Error persisting dispatch: {}", &e),
    }

    Ok(FileEvent {
        file_id: file_event.file_id,
        source_name: target_name.clone(),
        path: target_path,
        hash: file_event.hash.clone(),
    })
}
