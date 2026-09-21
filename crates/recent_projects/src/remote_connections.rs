use std::{
    cell::Cell,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use anyhow::{Context as _, Result};
use askpass::EncryptedPassword;
use editor::Editor;
use futures::{FutureExt as _, channel::oneshot, select};
use gpui::{AppContext, AsyncApp, Context, PromptLevel, Task, Window, WindowHandle};

use project::trusted_worktrees;
use remote::{
    DockerConnectionOptions, Interactive, RemoteConnection, RemoteConnectionOptions,
    SshConnectionOptions,
};
pub use settings::SshConnection;
use settings::{DevContainerConnection, ExtendingVec, RegisterSetting, Settings, WslConnection};
use util::paths::PathWithPosition;
use workspace::{
    AppState, MultiWorkspace, OpenOptions, SerializedWorkspaceLocation, Workspace,
    find_existing_workspace, with_remote_workspace_replacement,
};

pub use remote_connection::{
    RemoteClientDelegate, RemoteConnectionModal, RemoteConnectionPrompt, SshConnectionHeader,
    connect,
};

#[derive(RegisterSetting)]
pub struct RemoteSettings {
    pub ssh_connections: ExtendingVec<SshConnection>,
    pub wsl_connections: ExtendingVec<WslConnection>,
    /// Whether to read ~/.ssh/config for ssh connection sources.
    pub read_ssh_config: bool,
}

impl RemoteSettings {
    pub fn ssh_connections(&self) -> impl Iterator<Item = SshConnection> + use<> {
        self.ssh_connections.clone().0.into_iter()
    }

    pub fn wsl_connections(&self) -> impl Iterator<Item = WslConnection> + use<> {
        self.wsl_connections.clone().0.into_iter()
    }

    pub fn fill_connection_options_from_settings(&self, options: &mut SshConnectionOptions) {
        for conn in self.ssh_connections() {
            if conn.host == options.host.to_string()
                && conn.username == options.username
                && conn.port == options.port
            {
                options.nickname = conn.nickname;
                options.upload_binary_over_ssh = conn.upload_binary_over_ssh.unwrap_or_default();
                options.args = Some(conn.args);
                options.port_forwards = conn.port_forwards;
                break;
            }
        }
    }

    pub fn connection_options_for(
        &self,
        host: String,
        port: Option<u16>,
        username: Option<String>,
    ) -> SshConnectionOptions {
        let mut options = SshConnectionOptions {
            host: host.into(),
            port,
            username,
            ..Default::default()
        };
        self.fill_connection_options_from_settings(&mut options);
        options
    }
}

#[derive(Clone, PartialEq)]
pub enum Connection {
    Ssh(SshConnection),
    Wsl(WslConnection),
    DevContainer(DevContainerConnection),
}

impl From<Connection> for RemoteConnectionOptions {
    fn from(val: Connection) -> Self {
        match val {
            Connection::Ssh(conn) => RemoteConnectionOptions::Ssh(conn.into()),
            Connection::Wsl(conn) => RemoteConnectionOptions::Wsl(conn.into()),
            Connection::DevContainer(conn) => {
                RemoteConnectionOptions::Docker(DockerConnectionOptions {
                    name: conn.name,
                    remote_user: conn.remote_user,
                    container_id: conn.container_id,
                    upload_binary_over_docker_exec: false,
                    use_podman: conn.use_podman,
                    remote_env: conn.remote_env,
                })
            }
        }
    }
}

impl From<SshConnection> for Connection {
    fn from(val: SshConnection) -> Self {
        Connection::Ssh(val)
    }
}

impl From<WslConnection> for Connection {
    fn from(val: WslConnection) -> Self {
        Connection::Wsl(val)
    }
}

impl Settings for RemoteSettings {
    fn from_settings(content: &settings::SettingsContent) -> Self {
        let remote = &content.remote;
        Self {
            ssh_connections: remote.ssh_connections.clone().unwrap_or_default().into(),
            wsl_connections: remote.wsl_connections.clone().unwrap_or_default().into(),
            read_ssh_config: remote.read_ssh_config.unwrap(),
        }
    }
}

pub async fn open_remote_project(
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    open_options: workspace::OpenOptions,
    cx: &mut AsyncApp,
) -> Result<Option<WindowHandle<MultiWorkspace>>> {
    prepare_remote_project(connection_options, paths, app_state, open_options, cx)
        .await?
        .await
}

pub async fn prepare_remote_project(
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    open_options: workspace::OpenOptions,
    cx: &mut AsyncApp,
) -> Result<Task<Result<Option<WindowHandle<MultiWorkspace>>>>> {
    let created_new_window = open_options.requesting_window.is_none();
    let focus = open_options.focus.unwrap_or(true);

    let (existing, open_visible) = find_existing_workspace(
        &paths,
        &open_options,
        &SerializedWorkspaceLocation::Remote(connection_options.clone()),
        cx,
    )
    .await;

    if let Some((existing_window, existing_workspace)) = existing {
        let remote_connection = cx.update(|cx| {
            existing_workspace
                .read(cx)
                .project()
                .read(cx)
                .remote_client()
                .and_then(|client| client.read(cx).remote_connection())
        });

        if let Some(remote_connection) = remote_connection {
            let (resolved_paths, paths_with_positions) =
                determine_paths_with_positions(&remote_connection, paths).await;

            let open_paths = |multi_workspace: &mut MultiWorkspace,
                              window: &mut Window,
                              cx: &mut Context<MultiWorkspace>| {
                if focus {
                    window.activate_window();
                    multi_workspace.activate(existing_workspace.clone(), None, window, cx);
                }
                existing_workspace.update(cx, |workspace, cx| {
                    workspace.open_paths(
                        resolved_paths,
                        OpenOptions {
                            visible: Some(open_visible),
                            focus: open_options.focus,
                            ..Default::default()
                        },
                        None,
                        window,
                        cx,
                    )
                })
            };
            let open_paths = if focus {
                with_remote_workspace_replacement(
                    existing_window,
                    Some(&existing_workspace),
                    cx,
                    open_paths,
                )
                .await?
            } else {
                Some(existing_window.update(cx, open_paths)?)
            };
            let Some(open_paths) = open_paths else {
                return Ok(Task::ready(Ok(None)));
            };
            let open_results = open_paths.await;

            existing_workspace.update(cx, |workspace, cx| {
                for item in open_results.iter().flatten() {
                    if let Err(error) = item {
                        workspace.show_error(format!("{error}"), cx);
                    }
                }
            });

            let items = open_results
                .into_iter()
                .map(|r| r.and_then(|r| r.ok()))
                .collect::<Vec<_>>();
            navigate_to_positions(&existing_window, items, &paths_with_positions, cx);

            return Ok(Task::ready(Ok(Some(existing_window))));
        }
        // If the remote connection is dead (e.g. server not running after failed reconnect),
        // fall through to establish a fresh connection instead of showing an error.
        log::info!(
            "existing remote workspace found but connection is dead, starting fresh connection"
        );
    }

    let (window, initial_workspace) = if let Some(window) = open_options.requesting_window {
        let workspace = window.update(cx, |multi_workspace, _, _| {
            multi_workspace.workspace().clone()
        })?;
        (window, workspace)
    } else {
        let workspace_position = cx
            .update(|cx| {
                workspace::remote_workspace_position_from_db(connection_options.clone(), &paths, cx)
            })
            .await
            .context("fetching remote workspace position from db")?;

        let mut options =
            cx.update(|cx| (app_state.build_window_options)(workspace_position.display, cx));
        options.window_bounds = workspace_position.window_bounds;
        if !focus {
            options.show = true;
            options.focus = false;
        }

        let window = cx.open_window(options, |window, cx| {
            let project = project::Project::local(
                app_state.client.clone(),
                app_state.node_runtime.clone(),
                app_state.user_store.clone(),
                app_state.languages.clone(),
                app_state.fs.clone(),
                None,
                project::LocalProjectFlags {
                    init_worktree_trust: false,
                    ..Default::default()
                },
                cx,
            );
            let workspace = cx.new(|cx| {
                let mut workspace = Workspace::new(None, project, app_state.clone(), window, cx);
                workspace.centered_layout = workspace_position.centered_layout;
                workspace
            });
            cx.new(|cx| MultiWorkspace::new(workspace, window, cx))
        })?;
        let workspace = window.update(cx, |multi_workspace, _, _cx| {
            multi_workspace.workspace().clone()
        })?;
        (window, workspace)
    };

    if focus {
        window.update(cx, |_, window, _| window.activate_window())?;
    }

    Ok(cx.spawn(async move |cx| {
        let window_reused = Rc::new(Cell::new(false));
        loop {
            let (cancel_tx, mut cancel_rx) = oneshot::channel();
            let connection_ui = window.update(cx, {
                let paths = paths.clone();
                let connection_options = connection_options.clone();
                let initial_workspace = initial_workspace.clone();
                move |_multi_workspace: &mut MultiWorkspace, window, cx| {
                    initial_workspace.update(cx, |workspace, cx| {
                        workspace.hide_modal(window, cx);
                        let mut modal = None;
                        workspace.toggle_modal(window, cx, |window, cx| {
                            modal = Some(cx.entity());
                            RemoteConnectionModal::new(&connection_options, paths, window, cx)
                        });

                        let modal = modal?;
                        let ui = modal.read(cx).prompt.clone();

                        ui.update(cx, |ui, _cx| {
                            ui.set_cancellation_tx(cancel_tx);
                        });

                        Some((
                            modal.downgrade(),
                            Arc::new(RemoteClientDelegate::new(
                                window.window_handle(),
                                ui.downgrade(),
                                if let RemoteConnectionOptions::Ssh(options) = &connection_options {
                                    options
                                        .password
                                        .as_deref()
                                        .and_then(|pw| EncryptedPassword::try_from(pw).ok())
                                } else {
                                    None
                                },
                            )),
                        ))
                    })
                }
            })?;

            let Some((modal, delegate)) = connection_ui else {
                break;
            };
            let _reuse_subscription = created_new_window.then(|| {
                let window_reused = window_reused.clone();
                cx.update(|cx| {
                    cx.subscribe(&initial_workspace, move |_, event, _| {
                        if let workspace::Event::ModalOpened = event {
                            window_reused.set(true);
                        }
                    })
                })
            });

            let connection = remote::connect(connection_options.clone(), delegate.clone(), cx);
            let connection = select! {
                _ = cancel_rx => {
                    modal.update(cx, |modal, cx| modal.finished(cx)).ok();
                    break;
                },
                result = connection.fuse() => result,
            };
            let remote_connection = match connection {
                Ok(connection) => connection,
                Err(e) => {
                    modal.update(cx, |modal, cx| modal.finished(cx)).ok();
                    log::error!("Failed to open project: {e:#}");
                    let response = window
                        .update(cx, |_, window, cx| {
                            window.prompt(
                                PromptLevel::Critical,
                                match connection_options {
                                    RemoteConnectionOptions::Ssh(_) => "Failed to connect over SSH",
                                    RemoteConnectionOptions::Wsl(_) => "Failed to connect to WSL",
                                    RemoteConnectionOptions::Docker(_) => {
                                        "Failed to connect to Dev Container"
                                    }
                                    #[cfg(any(test, feature = "test-support"))]
                                    RemoteConnectionOptions::Mock(_) => {
                                        "Failed to connect to mock server"
                                    }
                                },
                                Some(&format!("{e:#}")),
                                &["Retry", "Cancel"],
                                cx,
                            )
                        })?
                        .await;

                    if response == Ok(0) {
                        continue;
                    }

                    break;
                }
            };

            let (paths, paths_with_positions) =
                determine_paths_with_positions(&remote_connection, paths.clone()).await;

            let opened = cx
                .update(|cx| {
                    workspace::open_remote_project_with_new_connection(
                        window,
                        remote_connection,
                        cancel_rx,
                        delegate.clone(),
                        app_state.clone(),
                        paths.clone(),
                        created_new_window.then(|| initial_workspace.clone()),
                        cx,
                        {
                            let modal = modal.clone();
                            move |cx| {
                                modal.update(cx, |modal, cx| modal.finished(cx)).ok();
                            }
                        },
                    )
                })
                .await;

            modal.update(cx, |modal, cx| modal.finished(cx)).ok();

            match opened {
                Err(e) => {
                    log::error!("Failed to open project: {e:#}");
                    let response = window
                        .update(cx, |_, window, cx| {
                            window.prompt(
                                PromptLevel::Critical,
                                match connection_options {
                                    RemoteConnectionOptions::Ssh(_) => "Failed to connect over SSH",
                                    RemoteConnectionOptions::Wsl(_) => "Failed to connect to WSL",
                                    RemoteConnectionOptions::Docker(_) => {
                                        "Failed to connect to Dev Container"
                                    }
                                    #[cfg(any(test, feature = "test-support"))]
                                    RemoteConnectionOptions::Mock(_) => {
                                        "Failed to connect to mock server"
                                    }
                                },
                                Some(&format!("{e:#}")),
                                &["Retry", "Cancel"],
                                cx,
                            )
                        })?
                        .await;
                    if response == Ok(0) {
                        continue;
                    }

                    initial_workspace.update(cx, |workspace, cx| {
                        trusted_worktrees::track_worktree_trust(
                            workspace.project().read(cx).worktree_store(),
                            None,
                            None,
                            None,
                            cx,
                        );
                    });
                }

                Ok((Some(_), items)) => {
                    navigate_to_positions(&window, items, &paths_with_positions, cx);
                    return Ok(Some(window));
                }
                Ok((None, _)) => {}
            }

            break;
        }

        if created_new_window && !window_reused.get() {
            window
                .update(cx, |multi_workspace, window, cx| {
                    if multi_workspace.workspace() == &initial_workspace
                        && multi_workspace.workspaces().count() == 1
                        && initial_workspace.read(cx).items(cx).next().is_none()
                        && initial_workspace
                            .read(cx)
                            .project()
                            .read(cx)
                            .visible_worktrees(cx)
                            .next()
                            .is_none()
                    {
                        window.remove_window();
                    }
                })
                .ok();
        }
        Ok(None)
    }))
}

pub fn navigate_to_positions(
    window: &WindowHandle<MultiWorkspace>,
    items: impl IntoIterator<Item = Option<Box<dyn workspace::item::ItemHandle>>>,
    positions: &[PathWithPosition],
    cx: &mut AsyncApp,
) {
    for (item, path) in items.into_iter().zip(positions) {
        let Some(item) = item else {
            continue;
        };
        let Some(row) = path.row else {
            continue;
        };
        if let Some(active_editor) = item.downcast::<Editor>() {
            window
                .update(cx, |_, window, cx| {
                    active_editor.update(cx, |editor, cx| {
                        let row = row.saturating_sub(1);
                        let col = path.column.unwrap_or(0).saturating_sub(1);
                        let Some(buffer) = editor.buffer().read(cx).as_singleton() else {
                            return;
                        };
                        let buffer_snapshot = buffer.read(cx).snapshot();
                        let point = buffer_snapshot.point_from_external_input(row, col);
                        editor.go_to_singleton_buffer_point(point, window, cx);
                    });
                })
                .ok();
        }
    }
}

pub(crate) async fn determine_paths_with_positions(
    remote_connection: &Arc<dyn RemoteConnection>,
    mut paths: Vec<PathBuf>,
) -> (Vec<PathBuf>, Vec<PathWithPosition>) {
    let mut paths_with_positions = Vec::<PathWithPosition>::new();
    for path in &mut paths {
        if let Some(path_str) = path.to_str() {
            let path_with_position = PathWithPosition::parse_str(&path_str);
            if path_with_position.row.is_some() {
                if !path_exists(&remote_connection, &path).await {
                    *path = path_with_position.path.clone();
                    paths_with_positions.push(path_with_position);
                    continue;
                }
            }
        }
        paths_with_positions.push(PathWithPosition::from_path(path.clone()))
    }
    (paths, paths_with_positions)
}

async fn path_exists(connection: &Arc<dyn RemoteConnection>, path: &Path) -> bool {
    let Ok(command) = connection.build_command(
        Some("test".to_string()),
        &["-e".to_owned(), path.to_string_lossy().to_string()],
        &Default::default(),
        None,
        None,
        Interactive::No,
    ) else {
        return false;
    };
    let Ok(mut child) = util::command::new_command(command.program)
        .args(command.args)
        .envs(command.env)
        .spawn()
    else {
        return false;
    };
    child.status().await.is_ok_and(|status| status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use extension::ExtensionHostProxy;
    use fs::FakeFs;
    use gpui::{AppContext, BorrowAppContext as _, Entity, Focusable as _, Task, TestAppContext};
    use http_client::BlockedHttpClient;
    use node_runtime::NodeRuntime;
    use open_path_prompt::{OpenPathDelegate, OpenPathPrompt};
    use picker::{Picker, PickerDelegate as _};
    use remote::{MockConnectionOptions, RemoteClient};
    use remote_server::{HeadlessAppState, HeadlessProject};
    use rpc::proto;
    use serde_json::json;
    use settings::SettingsStore;
    use util::{path, path_list::PathList};
    use workspace::{
        dock::{DockPosition, test::TestPanel},
        find_existing_workspace,
    };

    #[gpui::test]
    async fn test_open_remote_project_with_mock_connection(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        let paths = vec![PathBuf::from(path!("/project"))];
        let open_options = workspace::OpenOptions::default();

        let mut async_cx = cx.to_async();
        let result = open_remote_project(opts, paths, app_state, open_options, &mut async_cx).await;

        executor.run_until_parked();

        assert!(result.is_ok(), "open_remote_project should succeed");

        let windows = cx.update(|cx| cx.windows().len());
        assert_eq!(windows, 1, "Should have opened a window");

        let multi_workspace_handle =
            cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        multi_workspace_handle
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    let project = workspace.project().read(cx);
                    assert!(project.is_remote(), "Project should be a remote project");
                });
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_reuse_existing_remote_workspace_window(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                        "lib.rs": "pub fn hello() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        // First open: create a new window for the remote project.
        let paths = vec![PathBuf::from(path!("/project"))];
        let mut async_cx = cx.to_async();
        open_remote_project(
            opts.clone(),
            paths,
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("first open_remote_project should succeed");

        executor.run_until_parked();

        assert_eq!(
            cx.update(|cx| cx.windows().len()),
            1,
            "First open should create exactly one window"
        );

        let first_window = cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        // Verify find_existing_workspace discovers the remote workspace.
        let search_paths = vec![PathBuf::from(path!("/project/src/lib.rs"))];
        let (found, _open_visible) = find_existing_workspace(
            &search_paths,
            &workspace::OpenOptions::default(),
            &SerializedWorkspaceLocation::Remote(opts.clone()),
            &mut async_cx,
        )
        .await;

        assert!(
            found.is_some(),
            "find_existing_workspace should locate the existing remote workspace"
        );
        let (found_window, remote_workspace) = found.unwrap();
        assert_eq!(
            found_window, first_window,
            "find_existing_workspace should return the same window"
        );

        let scratch_project = project::Project::test(FakeFs::new(cx.executor()), [], cx).await;
        let (scratch_workspace, panel) = first_window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.retain_active_workspace(cx);
                let scratch_workspace = cx
                    .new(|cx| Workspace::new(None, scratch_project, app_state.clone(), window, cx));
                multi_workspace.add(scratch_workspace.clone(), window, cx);
                let panel = remote_workspace.update(cx, |workspace, cx| {
                    let panel = cx.new(|cx| TestPanel::new(DockPosition::Bottom, 0, cx));
                    workspace.add_panel(panel.clone(), window, cx);
                    panel
                });
                (scratch_workspace, panel)
            })
            .unwrap();
        let scratch_editor =
            add_scratch_editor(first_window, &scratch_workspace, "unsaved scratch text", cx);
        let scratch_session =
            scratch_workspace.read_with(cx, |workspace, _| workspace.session_id());
        let (foreground, _, _) = new_scratch_workspace(app_state.clone(), cx).await;
        for (focus, active_workspace) in [
            (Some(false), &remote_workspace),
            (Some(true), &remote_workspace),
            (None, &remote_workspace),
            (Some(false), &scratch_workspace),
        ] {
            let (requested_path, expected_text) = if active_workspace == &scratch_workspace {
                (PathBuf::from(path!("/project/src/main.rs")), "fn main() {}")
            } else {
                (
                    PathBuf::from(path!("/project/src/lib.rs")),
                    "pub fn hello() {}",
                )
            };
            let previous_focus = first_window
                .update(cx, |multi_workspace, window, cx| {
                    multi_workspace.activate(active_workspace.clone(), None, window, cx);
                    let focus_handle = if active_workspace == &remote_workspace {
                        remote_workspace.update(cx, |workspace, cx| {
                            workspace.focus_panel::<TestPanel>(window, cx);
                        });
                        panel.focus_handle(cx)
                    } else {
                        let focus_handle = scratch_editor.focus_handle(cx);
                        focus_handle.focus(window, cx);
                        focus_handle
                    };
                    assert_eq!(window.focused(cx), Some(focus_handle.clone()));
                    focus_handle
                })
                .unwrap();
            foreground
                .update(cx, |_, window, _| window.activate_window())
                .unwrap();
            let reused = open_remote_project(
                opts.clone(),
                vec![requested_path],
                app_state.clone(),
                OpenOptions {
                    focus,
                    ..OpenOptions::default()
                },
                &mut async_cx,
            )
            .await
            .unwrap();
            executor.run_until_parked();
            assert_eq!(reused, Some(first_window));
            assert_eq!(cx.windows().len(), 2);
            assert!(!cx.has_pending_prompt());
            first_window
                .update(cx, |multi_workspace, window, cx| {
                    assert_eq!(multi_workspace.workspace(), active_workspace);
                    assert_eq!(multi_workspace.workspaces().count(), 2);
                    let editor = remote_workspace
                        .read(cx)
                        .active_item_as::<Editor>(cx)
                        .unwrap();
                    assert_eq!(editor.read(cx).text(cx), expected_text);
                    let expected_focus = if focus == Some(false) {
                        previous_focus
                    } else {
                        editor.focus_handle(cx)
                    };
                    assert_eq!(window.focused(cx), Some(expected_focus));
                    assert_eq!(scratch_editor.read(cx).text(cx), "unsaved scratch text");
                    assert_eq!(scratch_workspace.read(cx).session_id(), scratch_session);
                    assert!(multi_workspace.is_workspace_retained(&scratch_workspace));
                    assert!(multi_workspace.is_workspace_retained(&remote_workspace));
                })
                .unwrap();
            let expected_foreground = if focus == Some(false) {
                foreground
            } else {
                first_window
            };
            assert_eq!(
                cx.read(|cx| cx.active_window()),
                Some(expected_foreground.into())
            );
        }
    }

    #[gpui::test]
    async fn test_reopen_existing_remote_root_treats_root_as_directory(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        let remote_home = paths::home_dir();
        let canonical_project_path = remote_home.join("remote-reopen-root-project");
        remote_fs
            .insert_tree(
                &canonical_project_path,
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        let mut async_cx = cx.to_async();
        let window = open_remote_project(
            opts,
            vec![canonical_project_path.clone()],
            app_state,
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("initial open_remote_project should succeed")
        .expect("initial open_remote_project should not be cancelled");

        executor.run_until_parked();

        let open_results = window
            .update(cx, |multi_workspace, window, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    workspace.open_paths(
                        vec![canonical_project_path.clone()],
                        workspace::OpenOptions {
                            visible: Some(workspace::OpenVisible::All),
                            ..Default::default()
                        },
                        None,
                        window,
                        cx,
                    )
                })
            })
            .unwrap()
            .await;

        assert_eq!(open_results.len(), 1, "should return one open result");
        assert!(
            open_results[0].is_none(),
            "reopening a remote root directory should not try to open it as a file"
        );
    }

    #[gpui::test]
    async fn test_reconnect_when_server_not_running(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let http_client = Arc::new(BlockedHttpClient);
        let node_runtime = NodeRuntime::unavailable();
        let languages = Arc::new(language::LanguageRegistry::new(server_cx.executor()));
        let proxy = Arc::new(ExtensionHostProxy::new());

        let _headless = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session,
                    fs: remote_fs.clone(),
                    http_client: http_client.clone(),
                    node_runtime: node_runtime.clone(),
                    languages: languages.clone(),
                    extension_host_proxy: proxy.clone(),
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard);

        // Open the remote project normally.
        let paths = vec![PathBuf::from(path!("/project"))];
        let mut async_cx = cx.to_async();
        open_remote_project(
            opts.clone(),
            paths.clone(),
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("initial open should succeed");

        executor.run_until_parked();

        assert_eq!(cx.update(|cx| cx.windows().len()), 1);
        let window = cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        // Force the remote client into ServerNotRunning state (simulates the
        // scenario where the remote server died and reconnection failed).
        window
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    let client = workspace
                        .project()
                        .read(cx)
                        .remote_client()
                        .expect("should have remote client");
                    client.update(cx, |client, cx| {
                        client.force_server_not_running(cx);
                    });
                });
            })
            .unwrap();

        executor.run_until_parked();

        // Register a new mock server under the same options so the reconnect
        // path can establish a fresh connection.
        let (server_session_2, connect_guard_2) =
            RemoteClient::fake_server_with_opts(&opts, cx, server_cx);

        let _headless_2 = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session: server_session_2,
                    fs: remote_fs.clone(),
                    http_client,
                    node_runtime,
                    languages,
                    extension_host_proxy: proxy,
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });

        drop(connect_guard_2);

        // Simulate clicking "Reconnect": calls open_remote_project with
        // replace_window pointing to the existing window.
        let result = open_remote_project(
            opts,
            paths,
            app_state,
            workspace::OpenOptions {
                requesting_window: Some(window),
                ..Default::default()
            },
            &mut async_cx,
        )
        .await;

        executor.run_until_parked();

        assert!(
            result.is_ok(),
            "reconnect should succeed but got: {:?}",
            result.err()
        );

        // Should still be a single window with a working remote project.
        assert_eq!(cx.update(|cx| cx.windows().len()), 1);

        window
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    assert!(
                        workspace.project().read(cx).is_remote(),
                        "project should be remote after reconnect"
                    );
                });
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_remote_replacement_preserves_scratch_on_failure_and_cancel(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
            cx.update_global::<SettingsStore, _>(|store, cx| {
                store
                    .set_user_settings(r#"{"disable_ai":true}"#, cx)
                    .unwrap();
            });
        });
        let (window, workspace, editor) = new_scratch_workspace(app_state.clone(), cx).await;
        let session_id = workspace.read_with(cx, |workspace, _| workspace.session_id());
        let open_options = OpenOptions {
            requesting_window: Some(window),
            ..OpenOptions::default()
        };
        let (connection_options, _server) = new_remote_project(cx, server_cx).await;
        for connection_options in [
            RemoteConnectionOptions::Mock(MockConnectionOptions { id: u64::MAX }),
            connection_options,
        ] {
            let failed_open = cx.spawn({
                let app_state = app_state.clone();
                let open_options = open_options.clone();
                async move |mut cx| {
                    open_remote_project(
                        connection_options,
                        vec![PathBuf::from(path!("/missing/project"))],
                        app_state,
                        open_options,
                        &mut cx,
                    )
                    .await
                }
            });
            cx.executor().run_until_parked();
            assert_eq!(
                cx.pending_prompt().map(|(message, _)| message),
                Some("Failed to connect to mock server".to_string())
            );
            cx.simulate_prompt_answer("Cancel");
            assert_eq!(failed_open.await.unwrap(), None);
            assert_eq!(
                window
                    .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
                    .unwrap(),
                workspace
            );
            assert_eq!(
                editor.read_with(cx, |editor, cx| editor.text(cx)),
                "unsaved scratch text"
            );
            assert_eq!(
                workspace.read_with(cx, |workspace, _| workspace.session_id()),
                session_id
            );
        }

        let (connection_options, _server) = new_remote_project(cx, server_cx).await;
        let remote_client = RemoteClient::connect_mock(connection_options.clone(), cx).await;
        let open = window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.find_or_create_workspace(
                    PathList::new(&[PathBuf::from(path!("/project"))]),
                    Some(connection_options),
                    None,
                    move |_, _, _| Task::ready(Ok(Some(remote_client))),
                    None,
                    workspace::OpenMode::Activate,
                    None,
                    window,
                    cx,
                )
            })
            .unwrap();
        cx.executor().run_until_parked();
        assert_eq!(
            cx.pending_prompt().map(|(message, _)| message),
            Some("This buffer contains unsaved edits. Do you want to save it?".to_string())
        );
        cx.simulate_prompt_answer("Cancel");
        assert!(open.await.unwrap().is_none());
        window
            .read_with(cx, |multi_workspace, cx| {
                assert_eq!(multi_workspace.workspace(), &workspace);
                assert!(!multi_workspace.is_workspace_retained(&workspace));
                assert_eq!(workspace.read(cx).session_id(), session_id);
                assert_eq!(editor.read(cx).text(cx), "unsaved scratch text");
                assert_eq!(multi_workspace.workspaces().count(), 1);
            })
            .unwrap();

        for answer in ["Cancel", "Don't Save"] {
            let (connection_options, _server) = new_remote_project(cx, server_cx).await;
            let open = cx.spawn({
                let app_state = app_state.clone();
                let open_options = open_options.clone();
                async move |mut cx| {
                    open_remote_project(
                        connection_options,
                        vec![PathBuf::from(path!("/project"))],
                        app_state,
                        open_options,
                        &mut cx,
                    )
                    .await
                }
            });
            cx.executor().run_until_parked();
            assert_eq!(
                cx.pending_prompt(),
                Some((
                    "This buffer contains unsaved edits. Do you want to save it?".to_string(),
                    String::new()
                ))
            );
            assert_eq!(
                window
                    .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
                    .unwrap(),
                workspace
            );
            assert_eq!(
                editor.read_with(cx, |editor, cx| editor.text(cx)),
                "unsaved scratch text"
            );
            cx.simulate_prompt_answer(answer);
            assert_eq!(
                open.await.unwrap(),
                (answer == "Don't Save").then_some(window)
            );
            assert!(!cx.has_pending_prompt());
            window
                .update(cx, |multi_workspace, _, cx| {
                    if answer == "Cancel" {
                        assert_eq!(multi_workspace.workspace(), &workspace);
                        assert_eq!(workspace.read(cx).session_id(), session_id);
                        assert_eq!(
                            workspace.read(cx).active_item_as::<Editor>(cx),
                            Some(editor.clone())
                        );
                        assert_eq!(editor.read(cx).text(cx), "unsaved scratch text");
                        assert!(
                            workspace
                                .read(cx)
                                .active_modal::<RemoteConnectionModal>(cx)
                                .is_none()
                        );
                    } else {
                        assert_ne!(multi_workspace.workspace(), &workspace);
                        assert!(
                            multi_workspace
                                .workspace()
                                .read(cx)
                                .project()
                                .read(cx)
                                .is_remote()
                        );
                        assert_eq!(multi_workspace.workspaces().count(), 1);
                    }
                })
                .unwrap();
        }
        assert_eq!(cx.windows().len(), 1);
    }

    #[gpui::test]
    async fn test_remote_add_preserves_foreground_scratch(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| {
            cx.update_global::<SettingsStore, _>(|store, cx| {
                store
                    .set_user_settings(r#"{"disable_ai":true}"#, cx)
                    .unwrap();
            });
        });
        let (window, scratch_workspace, editor) =
            new_scratch_workspace(app_state.clone(), cx).await;
        let session_id = scratch_workspace.read_with(cx, |workspace, _| workspace.session_id());
        assert!(session_id.is_some());
        let (connection_options, _server) = new_remote_project(cx, server_cx).await;
        let remote_client = RemoteClient::connect_mock(connection_options.clone(), cx).await;
        let paths = PathList::new(&[PathBuf::from(path!("/project"))]);
        let project_group =
            project::ProjectGroupKey::new(Some(connection_options.clone()), paths.clone());
        let mut remote_workspace: Option<Entity<Workspace>> = None;
        for reopen in [false, false, true] {
            let reopened_project = if reopen {
                let remote_workspace = remote_workspace.take().unwrap();
                let project =
                    remote_workspace.read_with(cx, |workspace, _| workspace.project().clone());
                window
                    .update(cx, |_, window, cx| {
                        remote_workspace.update(cx, |workspace, cx| {
                            workspace.open_abs_path(
                                PathBuf::from(path!("/project/file.txt")),
                                OpenOptions::default(),
                                window,
                                cx,
                            )
                        })
                    })
                    .unwrap()
                    .await
                    .unwrap();
                window
                    .update(cx, |_, window, cx| {
                        remote_workspace.update(cx, |workspace, cx| {
                            workspace.flush_serialization(window, cx)
                        })
                    })
                    .unwrap()
                    .await;
                assert!(
                    window
                        .update(cx, |multi_workspace, window, cx| {
                            multi_workspace.remove(
                                [remote_workspace],
                                workspace::RemovalIntent::CloseProject,
                                window,
                                cx,
                            )
                        })
                        .unwrap()
                        .await
                        .unwrap()
                );
                Some(project)
            } else {
                None
            };
            cx.executor().run_until_parked();
            window
                .update(cx, |_, window, cx| {
                    editor.focus_handle(cx).focus(window, cx);
                })
                .unwrap();
            let open = if let Some(project) = reopened_project {
                workspace::open_remote_project_with_existing_connection(
                    connection_options.clone(),
                    project,
                    paths.paths().to_vec(),
                    app_state.clone(),
                    window,
                    Some(project_group.clone()),
                    None,
                    workspace::OpenMode::Add,
                    &mut cx.to_async(),
                )
                .map(|result| result.map(|(workspace, _)| workspace))
                .boxed_local()
            } else {
                let remote_client = remote_workspace.is_none().then(|| remote_client.clone());
                window
                    .update(cx, |multi_workspace, window, cx| {
                        multi_workspace.find_or_create_workspace(
                            paths.clone(),
                            Some(connection_options.clone()),
                            Some(project_group.clone()),
                            move |_, _, _| {
                                Task::ready(Ok(Some(
                                    remote_client.expect("must reuse the workspace"),
                                )))
                            },
                            None,
                            workspace::OpenMode::Add,
                            None,
                            window,
                            cx,
                        )
                    })
                    .unwrap()
                    .boxed_local()
            };
            cx.executor().run_until_parked();
            assert_eq!(cx.pending_prompt(), None);
            let added_workspace = open.await.unwrap().unwrap();
            cx.executor().run_until_parked();
            if let Some(remote_workspace) = &remote_workspace {
                assert_eq!(&added_workspace, remote_workspace);
            }
            window
                .update(cx, |multi_workspace, window, cx| {
                    assert_eq!(multi_workspace.workspace(), &scratch_workspace);
                    assert_eq!(
                        multi_workspace.workspaces().cloned().collect::<Vec<_>>(),
                        vec![scratch_workspace.clone(), added_workspace.clone()]
                    );
                    assert!(multi_workspace.is_workspace_retained(&added_workspace));
                    assert!(!multi_workspace.is_workspace_retained(&scratch_workspace));
                    assert_eq!(scratch_workspace.read(cx).session_id(), session_id);
                    assert_eq!(
                        scratch_workspace.read(cx).active_item_as::<Editor>(cx),
                        Some(editor.clone())
                    );
                    assert_eq!(editor.read(cx).text(cx), "unsaved scratch text");
                    assert_eq!(
                        added_workspace.read(cx).root_paths(cx),
                        vec![Arc::<Path>::from(Path::new(path!("/project")))]
                    );
                    assert!(added_workspace.read(cx).project().read(cx).is_remote());
                    if reopen {
                        let restored_editor = added_workspace
                            .read(cx)
                            .active_item_as::<Editor>(cx)
                            .unwrap();
                        assert_eq!(restored_editor.read(cx).text(cx), "remote text");
                    }
                    assert_eq!(
                        window.focused(cx),
                        Some(editor.focus_handle(cx)),
                        "reopen={reopen}"
                    );
                })
                .unwrap();
            remote_workspace = Some(added_workspace);
        }
        assert_eq!(cx.windows().len(), 1);
    }

    #[gpui::test]
    async fn test_remote_open_prompts_for_retained_scratch(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let mut servers = Vec::new();
        for disable_ai in [true, false] {
            cx.update(|cx| {
                cx.update_global::<SettingsStore, _>(|store, cx| {
                    store
                        .set_user_settings(
                            &json!({"disable_ai": disable_ai, "agent": {"enabled": true}})
                                .to_string(),
                            cx,
                        )
                        .unwrap();
                });
            });
            let (window, workspace, editor) = new_scratch_workspace(app_state.clone(), cx).await;
            window
                .update(cx, |multi_workspace, _, cx| {
                    multi_workspace.retain_active_workspace(cx);
                })
                .unwrap();
            let session_id = workspace.read_with(cx, |workspace, _| workspace.session_id());
            let (connection_options, server) = new_remote_project(cx, server_cx).await;
            servers.push(server);
            let open_options = OpenOptions {
                requesting_window: Some(window),
                ..OpenOptions::default()
            };
            for answer in ["Cancel", "Don't Save"] {
                let open = cx.spawn({
                    let app_state = app_state.clone();
                    let open_options = open_options.clone();
                    let connection_options = connection_options.clone();
                    async move |mut cx| {
                        open_remote_project(
                            connection_options,
                            vec![PathBuf::from(path!("/project"))],
                            app_state,
                            open_options,
                            &mut cx,
                        )
                        .await
                    }
                });
                cx.executor().run_until_parked();
                assert_eq!(
                    cx.pending_prompt().map(|(message, _)| message),
                    Some("This buffer contains unsaved edits. Do you want to save it?".to_string())
                );
                cx.simulate_prompt_answer(answer);
                assert_eq!(
                    open.await.unwrap(),
                    (answer == "Don't Save").then_some(window)
                );
                window
                    .read_with(cx, |multi_workspace, cx| {
                        assert_eq!(multi_workspace.workspaces().count(), 1);
                        assert_eq!(editor.read(cx).text(cx), "unsaved scratch text");
                        if answer == "Cancel" {
                            assert_eq!(multi_workspace.workspace(), &workspace);
                            assert_eq!(workspace.read(cx).session_id(), session_id);
                        } else {
                            assert_ne!(multi_workspace.workspace(), &workspace);
                            assert!(!multi_workspace.is_workspace_retained(&workspace));
                            assert_eq!(workspace.read(cx).session_id(), None);
                        }
                    })
                    .unwrap();
            }
            let remote_workspace = window
                .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
                .unwrap();
            let remote_editor =
                add_scratch_editor(window, &remote_workspace, "unsaved scratch text", cx);
            open_remote_project(
                connection_options,
                vec![PathBuf::from(path!("/project"))],
                app_state.clone(),
                open_options,
                &mut cx.to_async(),
            )
            .await
            .unwrap();
            window
                .read_with(cx, |multi_workspace, cx| {
                    assert_eq!(multi_workspace.workspace(), &remote_workspace);
                    assert_eq!(remote_editor.read(cx).text(cx), "unsaved scratch text");
                    assert_eq!(multi_workspace.workspaces().count(), 1);
                })
                .unwrap();
            assert!(!cx.has_pending_prompt());
        }
    }

    #[gpui::test]
    async fn test_remote_reuse_cancels_when_scratch_changes_during_save(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| {
            cx.update_global::<SettingsStore, _>(|store, cx| {
                store
                    .set_user_settings(r#"{"disable_ai":true}"#, cx)
                    .unwrap();
            });
        });
        let (connection_options, _server) = new_remote_project(cx, server_cx).await;
        let window = open_remote_project(
            connection_options.clone(),
            vec![PathBuf::from(path!("/project"))],
            app_state.clone(),
            OpenOptions::default(),
            &mut cx.to_async(),
        )
        .await
        .unwrap()
        .unwrap();
        let project = project::Project::test(FakeFs::new(cx.executor()), [], cx).await;
        let (remote_workspace, scratch_workspace) = window
            .update(cx, |multi_workspace, window, cx| {
                let remote_workspace = multi_workspace.workspace().clone();
                multi_workspace.retain_active_workspace(cx);
                let scratch_workspace = cx
                    .new(|cx| Workspace::new(None, project.clone(), app_state.clone(), window, cx));
                multi_workspace.activate(scratch_workspace.clone(), None, window, cx);
                (remote_workspace, scratch_workspace)
            })
            .unwrap();
        let scratch_editor =
            add_scratch_editor(window, &scratch_workspace, "unsaved scratch text", cx);
        for mutation in ["add", "dirty", "edit"] {
            let clean_editor = (mutation == "dirty")
                .then(|| add_scratch_editor(window, &scratch_workspace, "", cx));
            let open = cx.spawn({
                let app_state = app_state.clone();
                let connection_options = connection_options.clone();
                async move |mut cx| {
                    open_remote_project(
                        connection_options,
                        vec![PathBuf::from(path!("/project"))],
                        app_state,
                        OpenOptions {
                            requesting_window: Some(window),
                            ..OpenOptions::default()
                        },
                        &mut cx,
                    )
                    .await
                }
            });
            cx.executor().run_until_parked();
            assert_eq!(
                cx.pending_prompt().map(|(message, _)| message),
                Some(if mutation == "add" {
                    "This buffer contains unsaved edits. Do you want to save it?".to_string()
                } else {
                    "Do you want to save all changes in the following files?".to_string()
                })
            );
            let changed_editor = if mutation == "add" {
                add_scratch_editor(window, &scratch_workspace, "new scratch text", cx)
            } else {
                let editor = clean_editor.unwrap_or_else(|| scratch_editor.clone());
                window
                    .update(cx, |_, window, cx| {
                        editor.update(cx, |editor, cx| editor.insert("new text", window, cx));
                    })
                    .unwrap();
                editor
            };
            let text = changed_editor.read_with(cx, |editor, cx| editor.text(cx));
            cx.simulate_prompt_answer(if mutation == "add" {
                "Don't Save"
            } else {
                "Discard all"
            });
            assert_eq!(open.await.unwrap(), None);
            assert_eq!(
                changed_editor.read_with(cx, |editor, cx| editor.text(cx)),
                text
            );
            window
                .read_with(cx, |multi_workspace, _| {
                    assert_eq!(multi_workspace.workspace(), &scratch_workspace);
                    assert_eq!(multi_workspace.workspaces().count(), 2);
                })
                .unwrap();
        }
        let open = cx.spawn({
            let app_state = app_state.clone();
            async move |mut cx| {
                open_remote_project(
                    connection_options,
                    vec![PathBuf::from(path!("/project"))],
                    app_state,
                    OpenOptions {
                        requesting_window: Some(window),
                        ..OpenOptions::default()
                    },
                    &mut cx,
                )
                .await
            }
        });
        cx.executor().run_until_parked();
        assert_eq!(
            cx.pending_prompt().map(|(message, _)| message),
            Some("Do you want to save all changes in the following files?".to_string())
        );
        let switched_workspace = window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.retain_active_workspace(cx);
                let workspace = cx.new(|cx| Workspace::new(None, project, app_state, window, cx));
                multi_workspace.activate(workspace.clone(), None, window, cx);
                workspace
            })
            .unwrap();
        let switched_editor =
            add_scratch_editor(window, &switched_workspace, "unsaved scratch text", cx);
        let session_id = switched_workspace.read_with(cx, |workspace, _| workspace.session_id());
        cx.simulate_prompt_answer("Discard all");
        assert_eq!(open.await.unwrap(), None);
        assert!(!cx.has_pending_prompt());
        window
            .read_with(cx, |multi_workspace, cx| {
                assert_eq!(multi_workspace.workspace(), &switched_workspace);
                assert!(!multi_workspace.is_workspace_retained(&switched_workspace));
                assert!(multi_workspace.is_workspace_retained(&remote_workspace));
                assert_eq!(switched_workspace.read(cx).session_id(), session_id);
                assert_eq!(switched_editor.read(cx).text(cx), "unsaved scratch text");
                assert_eq!(multi_workspace.workspaces().count(), 3);
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_remote_replacement_allows_builtin_save_as(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| {
            cx.observe_new(OpenPathPrompt::register_new_path).detach();
            cx.update_global::<SettingsStore, _>(|store, cx| {
                store
                    .set_user_settings(r#"{"disable_ai":true,"use_system_path_prompts":false}"#, cx)
                    .unwrap();
            });
        });
        app_state
            .fs
            .as_fake()
            .insert_tree(path!("/local"), json!({}))
            .await;

        for use_connection_helper in [false, true] {
            for save in [false, true] {
                let project = project::Project::test(app_state.fs.clone(), [], cx).await;
                let window = cx.add_window(|window, cx| {
                    let workspace =
                        cx.new(|cx| Workspace::new(None, project, app_state.clone(), window, cx));
                    MultiWorkspace::new(workspace, window, cx)
                });
                let workspace = window
                    .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
                    .unwrap();
                let editor = add_scratch_editor(window, &workspace, "saved scratch text\n", cx);
                let session_id = workspace.read_with(cx, |workspace, _| workspace.session_id());
                let (connection_options, _server) = new_remote_project(cx, server_cx).await;
                let open = if use_connection_helper {
                    window
                        .update(cx, |multi_workspace, window, cx| {
                            let workspace = workspace.clone();
                            multi_workspace.find_or_create_workspace(
                                PathList::new(&[PathBuf::from(path!("/project"))]),
                                Some(connection_options),
                                None,
                                move |options, window, cx| {
                                    remote_connection::connect_with_modal(
                                        &workspace, options, window, cx,
                                    )
                                },
                                None,
                                workspace::OpenMode::Activate,
                                None,
                                window,
                                cx,
                            )
                        })
                        .unwrap()
                        .map(|result| result.map(|workspace| workspace.map(|_| window)))
                        .boxed_local()
                } else {
                    cx.spawn({
                        let app_state = app_state.clone();
                        async move |mut cx| {
                            open_remote_project(
                                connection_options,
                                vec![PathBuf::from(path!("/project"))],
                                app_state,
                                OpenOptions {
                                    requesting_window: Some(window),
                                    ..OpenOptions::default()
                                },
                                &mut cx,
                            )
                            .await
                        }
                    })
                    .boxed_local()
                };
                cx.run_until_parked();
                cx.simulate_prompt_answer("Save");
                cx.run_until_parked();
                let picker = workspace
                    .read_with(cx, |workspace, cx| {
                        workspace.active_modal::<Picker<OpenPathDelegate>>(cx)
                    })
                    .expect("Save must open the built-in Save As picker");
                let saved_path = PathBuf::from(path!("/local"))
                    .join(format!("scratch-{use_connection_helper}-{save}.txt"));
                if save {
                    window
                        .update(cx, |_, window, cx| {
                            picker.update(cx, |picker, cx| {
                                picker.delegate.update_matches(
                                    saved_path.to_string_lossy().into_owned(),
                                    window,
                                    cx,
                                )
                            })
                        })
                        .unwrap()
                        .await;
                    window
                        .update(cx, |_, window, cx| {
                            picker.update(cx, |picker, cx| {
                                picker.delegate.confirm(false, window, cx);
                            });
                        })
                        .unwrap();
                } else {
                    window
                        .update(cx, |_, window, cx| {
                            picker.update(cx, |picker, cx| {
                                picker.cancel(&menu::Cancel, window, cx);
                            });
                        })
                        .unwrap();
                }
                assert_eq!(open.await.unwrap(), save.then_some(window));
                assert_eq!(
                    editor.read_with(cx, |editor, cx| editor.text(cx)),
                    "saved scratch text\n"
                );
                assert_eq!(
                    editor.read_with(cx, |editor, cx| editor.buffer().read(cx).is_dirty(cx)),
                    !save
                );
                if save {
                    assert_eq!(
                        app_state.fs.load(&saved_path).await.unwrap(),
                        "saved scratch text\n"
                    );
                    window
                        .read_with(cx, |multi_workspace, cx| {
                            assert_ne!(multi_workspace.workspace(), &workspace);
                            assert!(
                                multi_workspace
                                    .workspace()
                                    .read(cx)
                                    .project()
                                    .read(cx)
                                    .is_remote()
                            );
                        })
                        .unwrap();
                } else {
                    window
                        .read_with(cx, |multi_workspace, cx| {
                            assert_eq!(multi_workspace.workspace(), &workspace);
                            assert_eq!(workspace.read(cx).session_id(), session_id);
                            assert_eq!(
                                workspace.read(cx).active_item_as::<Editor>(cx),
                                Some(editor)
                            );
                        })
                        .unwrap();
                    assert!(app_state.fs.metadata(&saved_path).await.unwrap().is_none());
                }
                window
                    .update(cx, |_, window, _| window.remove_window())
                    .unwrap();
            }
        }
    }

    #[gpui::test]
    async fn test_remote_open_failure_preserves_new_connection_modal(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        for create_new_window in [false, true] {
            for second_connected in [None, Some(true), Some(false)] {
                let scratch = if create_new_window {
                    None
                } else {
                    Some(new_scratch_workspace(app_state.clone(), cx).await)
                };
                let requesting_window = scratch.as_ref().map(|(window, _, _)| *window);
                let (first_options, first_session, first_connect_guard) =
                    RemoteClient::fake_server(cx, server_cx);
                let (second_options, second_session, second_connect_guard) =
                    RemoteClient::fake_server(cx, server_cx);
                let mut second_connect_guard = Some(second_connect_guard);
                let [
                    (_first_server, first_started, first_finish),
                    (_second_server, second_started, second_finish),
                ] = [first_session, second_session].map(|session| {
                    let (started_sender, started_receiver) = oneshot::channel();
                    let (finish_sender, finish_receiver) = oneshot::channel();
                    let server =
                        server_cx.new(|_| (Some(started_sender), finish_receiver.shared()));
                    session.add_request_handler::<proto::Ping, _, _, _>(
                        server.downgrade(),
                        |_, _, _| async { Ok(proto::Ack {}) },
                    );
                    session.add_request_handler::<proto::AddWorktree, _, _, _>(
                        server.downgrade(),
                        |server, _, mut cx| async move {
                            let finish_receiver = server.update(&mut cx, |channels, _| {
                                if let Some(started_sender) = channels.0.take() {
                                    started_sender.send(()).unwrap();
                                }
                                channels.1.clone()
                            });
                            finish_receiver.await.unwrap();
                            anyhow::bail!("project opening failed")
                        },
                    );
                    (server, started_receiver, finish_sender)
                });
                drop(first_connect_guard);
                let open = cx.spawn({
                    let app_state = app_state.clone();
                    async move |mut cx| {
                        open_remote_project(
                            first_options,
                            vec![PathBuf::from(path!("/project"))],
                            app_state,
                            OpenOptions {
                                requesting_window,
                                ..OpenOptions::default()
                            },
                            &mut cx,
                        )
                        .await
                    }
                });
                first_started.await.unwrap();
                cx.run_until_parked();
                assert_eq!(cx.windows().len(), 1);
                let window = cx.windows()[0].downcast::<MultiWorkspace>().unwrap();
                let workspace = window
                    .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
                    .unwrap();
                assert!(workspace.read_with(cx, |workspace, cx| {
                    workspace
                        .active_modal::<RemoteConnectionModal>(cx)
                        .is_none()
                }));

                let mut second_open = None;
                let mut second_modal = None;
                if let Some(connected) = second_connected {
                    second_open = Some(
                        window
                            .update(cx, |multi_workspace, window, cx| {
                                let workspace = workspace.clone();
                                multi_workspace.find_or_create_workspace(
                                    PathList::new(&[PathBuf::from(path!("/project"))]),
                                    Some(second_options),
                                    None,
                                    move |options, window, cx| {
                                        remote_connection::connect_with_modal(
                                            &workspace, options, window, cx,
                                        )
                                    },
                                    None,
                                    workspace::OpenMode::Activate,
                                    None,
                                    window,
                                    cx,
                                )
                            })
                            .unwrap(),
                    );
                    cx.run_until_parked();
                    second_modal = Some(workspace.read_with(cx, |workspace, cx| {
                        workspace
                            .active_modal::<RemoteConnectionModal>(cx)
                            .unwrap()
                            .downgrade()
                    }));
                    if connected {
                        drop(second_connect_guard.take());
                        second_started.await.unwrap();
                        cx.run_until_parked();
                        assert!(workspace.read_with(cx, |workspace, cx| {
                            workspace
                                .active_modal::<RemoteConnectionModal>(cx)
                                .is_none()
                        }));
                    }
                    assert!(second_open.as_mut().unwrap().now_or_never().is_none());
                }

                first_finish.send(()).unwrap();
                cx.run_until_parked();
                if second_connected != Some(false) {
                    cx.simulate_prompt_answer("Retry");
                    cx.run_until_parked();
                }
                cx.simulate_prompt_answer("Cancel");
                assert_eq!(open.await.unwrap(), None);
                cx.run_until_parked();
                let keep_window = !create_new_window || second_connected.is_some();
                assert_eq!(
                    cx.windows().len(),
                    usize::from(keep_window),
                    "create_new_window={create_new_window}, second_connected={second_connected:?}"
                );
                if keep_window {
                    window
                        .read_with(cx, |multi_workspace, _| {
                            assert_eq!(multi_workspace.workspace(), &workspace);
                        })
                        .unwrap();
                }
                if let Some((_, _, editor)) = scratch {
                    assert_eq!(
                        editor.read_with(cx, |editor, cx| editor.text(cx)),
                        "unsaved scratch text"
                    );
                }
                if let Some(mut second_open) = second_open {
                    let second_modal = second_modal.unwrap();
                    assert_eq!(
                        workspace.read_with(cx, |workspace, cx| {
                            workspace
                                .active_modal::<RemoteConnectionModal>(cx)
                                .map(|modal| modal.entity_id())
                        }),
                        (second_connected == Some(false)).then_some(second_modal.entity_id())
                    );
                    assert!((&mut second_open).now_or_never().is_none());
                    if second_connected == Some(true) {
                        second_finish.send(()).unwrap();
                        assert!(second_open.await.is_err());
                    } else {
                        second_modal
                            .update(cx, |modal, cx| modal.finished(cx))
                            .unwrap();
                        assert!(second_open.await.unwrap().is_none());
                    }
                }
                if keep_window {
                    window
                        .update(cx, |_, window, _| window.remove_window())
                        .unwrap();
                }
            }
        }
    }

    async fn new_scratch_workspace(
        app_state: Arc<AppState>,
        cx: &mut TestAppContext,
    ) -> (
        WindowHandle<MultiWorkspace>,
        Entity<Workspace>,
        Entity<Editor>,
    ) {
        let project = project::Project::test(FakeFs::new(cx.executor()), [], cx).await;
        let window = cx.add_window(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(None, project, app_state, window, cx));
            MultiWorkspace::new(workspace, window, cx)
        });
        let workspace = window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .unwrap();
        let editor = add_scratch_editor(window, &workspace, "unsaved scratch text", cx);
        (window, workspace, editor)
    }

    fn add_scratch_editor(
        window: WindowHandle<MultiWorkspace>,
        workspace: &Entity<Workspace>,
        text: &str,
        cx: &mut TestAppContext,
    ) -> Entity<Editor> {
        window
            .update(cx, |_, window, cx| {
                workspace.update(cx, |workspace, cx| {
                    let buffer = cx.new(|cx| language::Buffer::local("", cx));
                    let editor = cx.new(|cx| {
                        Editor::for_buffer(
                            buffer.clone(),
                            Some(workspace.project().clone()),
                            window,
                            cx,
                        )
                    });
                    editor.update(cx, |editor, cx| editor.insert(text, window, cx));
                    assert_eq!(buffer.read(cx).is_dirty(), !text.is_empty());
                    workspace.add_item_to_active_pane(
                        Box::new(editor.clone()),
                        None,
                        true,
                        window,
                        cx,
                    );
                    editor
                })
            })
            .unwrap()
    }

    async fn new_remote_project(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) -> (RemoteConnectionOptions, Entity<HeadlessProject>) {
        cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
            HeadlessProject::init(cx);
        });
        let (connection_options, session, connect_guard) = RemoteClient::fake_server(cx, server_cx);
        let fs = FakeFs::new(server_cx.executor());
        fs.insert_tree(path!("/project"), json!({"file.txt": "remote text"}))
            .await;
        let server = server_cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session,
                    fs,
                    http_client: Arc::new(BlockedHttpClient),
                    node_runtime: NodeRuntime::unavailable(),
                    languages: Arc::new(language::LanguageRegistry::new(
                        cx.background_executor().clone(),
                    )),
                    extension_host_proxy: Arc::new(ExtensionHostProxy::new()),
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        });
        drop(connect_guard);
        (connection_options, server)
    }

    fn init_test(cx: &mut TestAppContext) -> Arc<AppState> {
        cx.update(|cx| {
            let state = AppState::test(cx);
            crate::init(cx);
            editor::init(cx);
            state
        })
    }
}
