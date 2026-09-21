use anyhow::{Context as _, bail};
use futures::{
    FutureExt, StreamExt as _,
    channel::{mpsc, oneshot},
    future::Shared,
};
use language::Buffer;
use remote::RemoteClient;
use rpc::proto::{self, REMOTE_SERVER_PROJECT_ID};
use std::{collections::VecDeque, path::Path, sync::Arc};
use task::{Shell, shell_to_proto};
use util::{ResultExt, command::new_command};
use worktree::Worktree;

use collections::HashMap;
use gpui::{App, AppContext as _, Context, Entity, EventEmitter, Task, WeakEntity};
use settings::Settings as _;

use crate::{
    project_settings::{DirenvSettings, ProjectSettings},
    worktree_store::WorktreeStore,
};

pub struct ProjectEnvironment {
    cli_environment: Option<HashMap<String, String>>,
    pending_environment: Option<Shared<Task<()>>>,
    weak_self: WeakEntity<Self>,
    local_environments: HashMap<(Shell, Arc<Path>), Shared<Task<Option<HashMap<String, String>>>>>,
    remote_environments: HashMap<(Shell, Arc<Path>), Shared<Task<Option<HashMap<String, String>>>>>,
    environment_error_messages: VecDeque<String>,
    environment_error_messages_tx: mpsc::UnboundedSender<String>,
    worktree_store: WeakEntity<WorktreeStore>,
    remote_client: Option<WeakEntity<RemoteClient>>,
    is_remote_project: bool,
    _tasks: Vec<Task<()>>,
}

pub enum ProjectEnvironmentEvent {
    ErrorsUpdated,
}

impl EventEmitter<ProjectEnvironmentEvent> for ProjectEnvironment {}

impl ProjectEnvironment {
    pub fn new(
        cli_environment: Option<HashMap<String, String>>,
        worktree_store: WeakEntity<WorktreeStore>,
        remote_client: Option<WeakEntity<RemoteClient>>,
        is_remote_project: bool,
        cx: &mut Context<Self>,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded();
        let task = cx.spawn(async move |this, cx| {
            while let Some(message) = rx.next().await {
                this.update(cx, |this, cx| {
                    this.environment_error_messages.push_back(message);
                    cx.emit(ProjectEnvironmentEvent::ErrorsUpdated);
                })
                .ok();
            }
        });
        Self {
            cli_environment,
            pending_environment: None,
            weak_self: cx.weak_entity(),
            local_environments: Default::default(),
            remote_environments: Default::default(),
            environment_error_messages: Default::default(),
            environment_error_messages_tx: tx,
            worktree_store,
            remote_client,
            is_remote_project,
            _tasks: vec![task],
        }
    }

    /// Returns the inherited CLI environment, if this project was opened from the Zed CLI.
    pub(crate) fn get_cli_environment(&self) -> Option<HashMap<String, String>> {
        if let Some(mut env) = self.cli_environment.clone() {
            set_origin_marker(&mut env, EnvironmentOrigin::Cli);
            Some(env)
        } else if cfg!(any(test, feature = "test-support")) {
            Some(HashMap::default())
        } else {
            None
        }
    }

    pub fn buffer_environment(
        &mut self,
        buffer: &Entity<Buffer>,
        worktree_store: &Entity<WorktreeStore>,
        cx: &mut Context<Self>,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(pending) = self.pending_environment.clone() {
            let buffer = buffer.clone();
            let worktree_store = worktree_store.clone();
            return self.after_environment_resolved(pending, cx, move |environment, cx| {
                environment.buffer_environment(&buffer, &worktree_store, cx)
            });
        }

        if let Some(cli_environment) = self.get_cli_environment() {
            log::debug!("using project environment variables from CLI");
            return Task::ready(Some(cli_environment)).shared();
        }

        let Some(worktree) = buffer
            .read(cx)
            .file()
            .map(|f| f.worktree_id(cx))
            .and_then(|worktree_id| worktree_store.read(cx).worktree_for_id(worktree_id, cx))
        else {
            return Task::ready(None).shared();
        };
        self.worktree_environment(worktree, cx)
    }

    pub fn worktree_environment(
        &mut self,
        worktree: Entity<Worktree>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(pending) = self.pending_environment.clone() {
            return self.after_environment_resolved(pending, cx, move |environment, cx| {
                environment.worktree_environment(worktree, cx)
            });
        }

        if let Some(cli_environment) = self.get_cli_environment() {
            log::debug!("using project environment variables from CLI");
            return Task::ready(Some(cli_environment)).shared();
        }

        let worktree = worktree.read(cx);
        let mut abs_path = worktree.abs_path();
        if worktree.is_single_file() {
            let Some(parent) = abs_path.parent() else {
                return Task::ready(None).shared();
            };
            abs_path = parent.into();
        }

        let remote_client = self.remote_client.as_ref().and_then(|it| it.upgrade());
        match remote_client {
            Some(remote_client) => remote_client.clone().read(cx).shell().map(|shell| {
                self.remote_directory_environment(
                    &Shell::Program(shell),
                    abs_path,
                    remote_client,
                    cx,
                )
            }),
            None if self.is_remote_project => {
                Some(self.local_directory_environment(&Shell::System, abs_path, cx))
            }
            None => Some(self.local_directory_environment(&Shell::System, abs_path, cx)),
        }
        .unwrap_or_else(|| Task::ready(None).shared())
    }

    pub fn directory_environment(
        &mut self,
        abs_path: Arc<Path>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(pending) = self.pending_environment.clone() {
            return self.after_environment_resolved(pending, cx, move |environment, cx| {
                environment.directory_environment(abs_path, cx)
            });
        }

        let remote_client = self.remote_client.as_ref().and_then(|it| it.upgrade());
        match remote_client {
            Some(remote_client) => remote_client.clone().read(cx).shell().map(|shell| {
                self.remote_directory_environment(
                    &Shell::Program(shell),
                    abs_path,
                    remote_client,
                    cx,
                )
            }),
            None if self.is_remote_project => {
                Some(self.local_directory_environment(&Shell::System, abs_path, cx))
            }
            None => self
                .worktree_store
                .read_with(cx, |worktree_store, cx| {
                    worktree_store.find_worktree(&abs_path, cx)
                })
                .ok()
                .map(|_| self.local_directory_environment(&Shell::System, abs_path, cx)),
        }
        .unwrap_or_else(|| Task::ready(None).shared())
    }

    /// Returns the project environment using the default worktree path.
    /// This ensures that project-specific environment variables (e.g. from `.envrc`)
    /// are loaded from the project directory rather than the home directory.
    pub fn default_environment(
        &mut self,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(pending) = self.pending_environment.clone() {
            return self.after_environment_resolved(pending, cx, |environment, cx| {
                environment.default_environment(cx)
            });
        }

        let abs_path = self
            .worktree_store
            .read_with(cx, |worktree_store, cx| {
                crate::Project::default_visible_worktree_paths(worktree_store, cx)
                    .into_iter()
                    .next()
            })
            .ok()
            .flatten()
            .map(|path| Arc::<Path>::from(path))
            .unwrap_or_else(|| paths::home_dir().as_path().into());
        self.local_directory_environment(&Shell::System, abs_path, cx)
    }

    /// Returns the project environment, if possible.
    /// If the project was opened from the CLI, then the inherited CLI environment is returned.
    /// If it wasn't opened from the CLI, and an absolute path is given, then a shell is spawned in
    /// that directory, to get environment variables as if the user has `cd`'d there.
    pub fn local_directory_environment(
        &mut self,
        shell: &Shell,
        abs_path: Arc<Path>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(pending) = self.pending_environment.clone() {
            let shell = shell.clone();
            return self.after_environment_resolved(pending, cx, move |environment, cx| {
                environment.local_directory_environment(&shell, abs_path, cx)
            });
        }

        if let Some(cli_environment) = self.get_cli_environment() {
            log::debug!("using project environment variables from CLI");
            return Task::ready(Some(cli_environment)).shared();
        }

        self.local_environments
            .entry((shell.clone(), abs_path.clone()))
            .or_insert_with(|| {
                let load_direnv = ProjectSettings::get_global(cx).load_direnv.clone();
                let shell = shell.clone();
                let tx = self.environment_error_messages_tx.clone();
                cx.spawn(async move |cx| {
                    let mut shell_env = match cx
                        .background_spawn(load_directory_shell_environment(
                            shell,
                            abs_path.clone(),
                            load_direnv,
                            tx,
                        ))
                        .await
                    {
                        Ok(shell_env) => Some(shell_env),
                        Err(e) => {
                            log::error!(
                                "Failed to load shell environment for directory {abs_path:?}: {e:#}"
                            );
                            None
                        }
                    };

                    if let Some(shell_env) = shell_env.as_mut() {
                        let path = shell_env
                            .get("PATH")
                            .map(|path| path.as_str())
                            .unwrap_or_default();
                        log::debug!(
                            "using project environment variables shell launched in {:?}. PATH={:?}",
                            abs_path,
                            path
                        );

                        set_origin_marker(shell_env, EnvironmentOrigin::WorktreeShell);
                    }

                    shell_env
                })
                .shared()
            })
            .clone()
    }

    pub fn remote_directory_environment(
        &mut self,
        shell: &Shell,
        abs_path: Arc<Path>,
        remote_client: Entity<RemoteClient>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if let Some(pending) = self.pending_environment.clone() {
            let shell = shell.clone();
            return self.after_environment_resolved(pending, cx, move |environment, cx| {
                environment.remote_directory_environment(&shell, abs_path, remote_client, cx)
            });
        }

        if cfg!(any(test, feature = "test-support")) {
            return Task::ready(Some(HashMap::default())).shared();
        }

        self.remote_environments
            .entry((shell.clone(), abs_path.clone()))
            .or_insert_with(|| {
                let response =
                    remote_client
                        .read(cx)
                        .proto_client()
                        .request(proto::GetDirectoryEnvironment {
                            project_id: REMOTE_SERVER_PROJECT_ID,
                            shell: Some(shell_to_proto(shell.clone())),
                            directory: abs_path.to_string_lossy().to_string(),
                        });
                cx.background_spawn(async move {
                    let environment = response.await.log_err()?;
                    Some(environment.environment.into_iter().collect())
                })
                .shared()
            })
            .clone()
    }

    pub fn peek_environment_error(&self) -> Option<&String> {
        self.environment_error_messages.front()
    }

    pub fn pop_environment_error(&mut self) -> Option<String> {
        self.environment_error_messages.pop_front()
    }

    pub(crate) fn defer_environment(
        &mut self,
        cx: &mut Context<Self>,
    ) -> (
        oneshot::Sender<Option<HashMap<String, String>>>,
        Shared<Task<()>>,
    ) {
        debug_assert!(self.pending_environment.is_none());
        debug_assert!(self.cli_environment.is_none());
        debug_assert!(self.local_environments.is_empty());
        debug_assert!(self.remote_environments.is_empty());
        let (sender, receiver) = oneshot::channel();
        let completion = cx
            .spawn(async move |environment, cx| {
                let cli_environment = receiver.await.unwrap_or_default();
                environment
                    .update(cx, |environment, _| {
                        environment.cli_environment = cli_environment;
                        environment.pending_environment = None;
                    })
                    .ok();
            })
            .shared();
        self.pending_environment = Some(completion.clone());
        (sender, completion)
    }

    fn after_environment_resolved(
        &self,
        pending: Shared<Task<()>>,
        cx: &mut App,
        resolve: impl FnOnce(
            &mut Self,
            &mut Context<Self>,
        ) -> Shared<Task<Option<HashMap<String, String>>>>
        + 'static,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        let environment = self.weak_self.clone();
        cx.spawn(async move |cx| {
            pending.await;
            environment.update(cx, resolve).ok()?.await
        })
        .shared()
    }
}

fn set_origin_marker(env: &mut HashMap<String, String>, origin: EnvironmentOrigin) {
    env.insert(ZED_ENVIRONMENT_ORIGIN_MARKER.to_string(), origin.into());
}

const ZED_ENVIRONMENT_ORIGIN_MARKER: &str = "ZED_ENVIRONMENT";

enum EnvironmentOrigin {
    Cli,
    WorktreeShell,
}

impl From<EnvironmentOrigin> for String {
    fn from(val: EnvironmentOrigin) -> Self {
        match val {
            EnvironmentOrigin::Cli => "cli".into(),
            EnvironmentOrigin::WorktreeShell => "worktree-shell".into(),
        }
    }
}

async fn load_directory_shell_environment(
    shell: Shell,
    abs_path: Arc<Path>,
    load_direnv: DirenvSettings,
    tx: mpsc::UnboundedSender<String>,
) -> anyhow::Result<HashMap<String, String>> {
    if let DirenvSettings::Disabled = load_direnv {
        return Ok(HashMap::default());
    }

    let meta = smol::fs::metadata(&abs_path).await.with_context(|| {
        tx.unbounded_send(format!("Failed to open {}", abs_path.display()))
            .ok();
        format!("stat {abs_path:?}")
    })?;

    let dir = if meta.is_dir() {
        abs_path.clone()
    } else {
        abs_path
            .parent()
            .with_context(|| {
                tx.unbounded_send(format!("Failed to open {}", abs_path.display()))
                    .ok();
                format!("getting parent of {abs_path:?}")
            })?
            .into()
    };

    let (shell, args) = shell.program_and_args();
    let mut envs = util::shell_env::capture(shell.clone(), args, abs_path)
        .await
        .with_context(|| {
            tx.unbounded_send("Failed to load environment variables".into())
                .ok();
            format!("capturing shell environment with {shell:?}")
        })?;

    if cfg!(target_os = "windows")
        && let Some(path) = envs.remove("Path")
    {
        // windows env vars are case-insensitive, so normalize the path var
        // so we can just assume `PATH` in other places
        envs.insert("PATH".into(), path);
    }
    // If the user selects `Direct` for direnv, it would set an environment
    // variable that later uses to know that it should not run the hook.
    // We would include in `.envs` call so it is okay to run the hook
    // even if direnv direct mode is enabled.
    let direnv_environment = match load_direnv {
        DirenvSettings::ShellHook => None,
        DirenvSettings::Disabled => bail!("direnv integration is disabled"),
        // Note: direnv is not available on Windows, so we skip direnv processing
        // and just return the shell environment
        DirenvSettings::Direct if cfg!(target_os = "windows") => None,
        DirenvSettings::Direct => load_direnv_environment(&envs, &dir)
            .await
            .with_context(|| {
                tx.unbounded_send("Failed to load direnv environment".into())
                    .ok();
                "load direnv environment"
            })
            .log_err(),
    };
    if let Some(direnv_environment) = direnv_environment {
        for (key, value) in direnv_environment {
            if let Some(value) = value {
                envs.insert(key, value);
            } else {
                envs.remove(&key);
            }
        }
    }

    Ok(envs)
}

async fn load_direnv_environment(
    env: &HashMap<String, String>,
    dir: &Path,
) -> anyhow::Result<HashMap<String, Option<String>>> {
    let Some(direnv_path) = which::which("direnv").ok() else {
        return Ok(HashMap::default());
    };

    let args = &["export", "json"];
    let direnv_output = new_command(&direnv_path)
        .args(args)
        .envs(env)
        .env("TERM", "dumb")
        .current_dir(dir)
        .output()
        .await
        .context("running direnv")?;

    if !direnv_output.status.success() {
        bail!(
            "Loading direnv environment failed ({}), stderr: {}",
            direnv_output.status,
            String::from_utf8_lossy(&direnv_output.stderr)
        );
    }

    let output = String::from_utf8_lossy(&direnv_output.stdout);
    if output.is_empty() {
        // direnv outputs nothing when it has no changes to apply to environment variables
        return Ok(HashMap::default());
    }

    serde_json::from_str(&output).context("parsing direnv json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worktree_store::WorktreeIdCounter;
    use fs::FakeFs;
    use gpui::TestAppContext;
    use serde_json::json;
    use settings::SettingsStore;
    use util::path;

    #[gpui::test]
    fn test_cli_environment(cx: &mut TestAppContext) {
        let fs = FakeFs::new(cx.executor());
        let worktree_store =
            cx.new(|_| WorktreeStore::local(false, fs, WorktreeIdCounter::default()));

        for (cli_environment, expected) in [
            (None, HashMap::default()),
            (
                Some(HashMap::default()),
                HashMap::from_iter([("ZED_ENVIRONMENT".to_owned(), "cli".to_owned())]),
            ),
            (
                Some(HashMap::from_iter([
                    ("CLI_SENTINEL".to_owned(), "value".to_owned()),
                    ("ZED_ENVIRONMENT".to_owned(), "worktree-shell".to_owned()),
                ])),
                HashMap::from_iter([
                    ("CLI_SENTINEL".to_owned(), "value".to_owned()),
                    ("ZED_ENVIRONMENT".to_owned(), "cli".to_owned()),
                ]),
            ),
        ] {
            let environment = cx.new(|cx| {
                ProjectEnvironment::new(
                    cli_environment,
                    worktree_store.downgrade(),
                    None,
                    false,
                    cx,
                )
            });
            assert_eq!(
                environment.read_with(cx, |environment, _| environment.get_cli_environment()),
                Some(expected)
            );
        }
    }

    #[gpui::test]
    async fn test_deferred_cli_environment(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings = SettingsStore::test(cx);
            cx.set_global(settings);
        });
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(path!("/project"), json!({})).await;
        let buffer = cx.new(|cx| Buffer::local("", cx));
        let root = Arc::<Path>::from(Path::new(path!("/project")));

        for (resolution, expected) in [
            (
                Some(Some(HashMap::from_iter([(
                    "CLI_SENTINEL".to_owned(),
                    "resolved".to_owned(),
                )]))),
                HashMap::from_iter([
                    ("CLI_SENTINEL".to_owned(), "resolved".to_owned()),
                    ("ZED_ENVIRONMENT".to_owned(), "cli".to_owned()),
                ]),
            ),
            (Some(None), HashMap::default()),
            (None, HashMap::default()),
        ] {
            let worktree_store =
                cx.new(|_| WorktreeStore::local(false, fs.clone(), WorktreeIdCounter::default()));
            let environment = cx.new(|cx| {
                ProjectEnvironment::new(None, worktree_store.downgrade(), None, false, cx)
            });
            let (sender, completion) =
                environment.update(cx, |environment, cx| environment.defer_environment(cx));
            let directory = environment.update(cx, |environment, cx| {
                environment.directory_environment(root.clone(), cx)
            });
            let worktree = worktree_store
                .update(cx, |store, cx| {
                    store.create_worktree(root.as_ref(), true, cx)
                })
                .await
                .unwrap();
            let requests = environment.update(cx, |environment, cx| {
                [
                    directory,
                    environment.buffer_environment(&buffer, &worktree_store, cx),
                    environment.worktree_environment(worktree, cx),
                    environment.default_environment(cx),
                    environment.local_directory_environment(&Shell::System, root.clone(), cx),
                ]
            });
            cx.run_until_parked();
            assert_eq!(completion.clone().now_or_never(), None);
            for request in &requests {
                assert_eq!(request.clone().now_or_never(), None);
            }
            environment.read_with(cx, |environment, _| {
                assert!(environment.local_environments.is_empty());
                assert!(environment.remote_environments.is_empty());
            });

            if let Some(cli_environment) = resolution {
                sender.send(cli_environment).unwrap();
            } else {
                drop(sender);
            }
            completion.await;
            environment.read_with(cx, |environment, _| {
                assert!(environment.pending_environment.is_none());
                assert_eq!(environment.get_cli_environment(), Some(expected.clone()));
            });
            for request in requests {
                assert_eq!(request.await, Some(expected.clone()));
            }
        }

        let worktree_store =
            cx.new(|_| WorktreeStore::local(false, fs, WorktreeIdCounter::default()));
        let environment =
            cx.new(|cx| ProjectEnvironment::new(None, worktree_store.downgrade(), None, false, cx));
        let (sender, completion) =
            environment.update(cx, |environment, cx| environment.defer_environment(cx));
        let request = environment.update(cx, |environment, cx| environment.default_environment(cx));
        let weak_environment = environment.downgrade();
        drop(environment);
        drop(sender);
        cx.run_until_parked();
        completion.await;
        assert!(weak_environment.upgrade().is_none());
        assert_eq!(request.await, None);
    }
}
