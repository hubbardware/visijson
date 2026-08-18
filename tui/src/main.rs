//! visijson-tui — a terminal client for a running visijson server.
//!
//! HTTP only, by design: it works against any server and exercises the same
//! API every other client uses. Live updates arrive over SSE (named events,
//! RFC 6902 patches applied to a local mirror, `Last-Event-ID` resume on
//! reconnect); edits go through PUT/POST/DELETE guarded by `If-Match`, so a
//! concurrent change surfaces as a conflict instead of a lost update.
//!
//! Usage:
//!   visijson-tui [http://127.0.0.1:8080]
//!   visijson-tui --check [URL]   # headless: connect, print state, exit
//!
//! Keys: ↑/k ↓/j move · ←/h →/l/Enter fold/unfold · e edit (`=expr` makes a
//! formula, other input is JSON, bare words become strings) · a add child ·
//! d delete · g/G top/bottom · q quit.

use std::collections::HashSet;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use serde_json::{json, Value};

// ------------------------------------------------------------- JSON pointers

fn escape_token(tok: &str) -> String {
    tok.replace('~', "~0").replace('/', "~1")
}

fn parse_pointer(p: &str) -> Vec<String> {
    if p.is_empty() {
        return Vec::new();
    }
    p[1..]
        .split('/')
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// Percent-encode a pointer for use in a URL path.
fn url_of(base: &str, ptr: &str) -> String {
    let mut out = format!("{base}/doc");
    for tok in parse_pointer(ptr) {
        out.push('/');
        for b in escape_token(&tok).bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
    }
    out
}

// --------------------------------------------------------- RFC 6902 (subset)

/// Apply the server's diff patches (add / replace / remove) to the mirror.
/// The server only emits ops consistent with the mirror's state, so any
/// mismatch is a bug worth surfacing — we return an error and the caller
/// falls back to a resync.
fn apply_patch(root: &mut Value, exists: &mut bool, patch: &[Value]) -> Result<(), String> {
    for op in patch {
        let kind = op["op"].as_str().ok_or("op missing")?;
        let path = op["path"].as_str().ok_or("path missing")?;
        let tokens = parse_pointer(path);
        if tokens.is_empty() {
            match kind {
                "add" | "replace" => {
                    *root = op["value"].clone();
                    *exists = true;
                }
                "remove" => {
                    *root = Value::Null;
                    *exists = false;
                }
                other => return Err(format!("op `{other}`")),
            }
            continue;
        }
        let (last, parents) = tokens.split_last().unwrap();
        let mut cur = &mut *root;
        for tok in parents {
            cur = match cur {
                Value::Object(m) => m.get_mut(tok).ok_or_else(|| format!("missing {tok}"))?,
                Value::Array(a) => {
                    let i: usize = tok.parse().map_err(|_| format!("bad index {tok}"))?;
                    a.get_mut(i).ok_or_else(|| format!("missing [{i}]"))?
                }
                _ => return Err(format!("cannot descend into scalar at {tok}")),
            };
        }
        match (kind, &mut *cur) {
            ("add", Value::Array(a)) => {
                let i = if last == "-" { a.len() } else { last.parse().map_err(|_| "bad index")? };
                if i > a.len() {
                    return Err("index out of range".into());
                }
                a.insert(i, op["value"].clone());
            }
            ("add" | "replace", Value::Object(m)) => {
                m.insert(last.clone(), op["value"].clone());
            }
            ("replace", Value::Array(a)) => {
                let i: usize = last.parse().map_err(|_| "bad index")?;
                *a.get_mut(i).ok_or("index out of range")? = op["value"].clone();
            }
            ("remove", Value::Object(m)) => {
                m.remove(last);
            }
            ("remove", Value::Array(a)) => {
                let i: usize = last.parse().map_err(|_| "bad index")?;
                if i >= a.len() {
                    return Err("index out of range".into());
                }
                a.remove(i);
            }
            (other, _) => return Err(format!("op `{other}` on unexpected target")),
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ SSE feed

enum Feed {
    Snapshot {
        version: u64,
        exists: bool,
        value: Value,
        resync: bool,
    },
    Change {
        version: u64,
        patch: Vec<Value>,
    },
    Formulas(HashSet<String>),
    Conn(String),
}

/// One parsed SSE frame: (event name, id, data).
fn parse_frame(frame: &str) -> Option<(String, Option<u64>, Value)> {
    let (mut name, mut id, mut data) = (None, None, None);
    for line in frame.lines() {
        if let Some(v) = line.strip_prefix("event:") {
            name = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("id:") {
            id = v.trim().parse().ok();
        } else if let Some(v) = line.strip_prefix("data:") {
            data = serde_json::from_str(v.trim()).ok();
        }
    }
    Some((name?, id, data?))
}

/// Fetch the formula locations from `GET /formulas` (path + expression off
/// the server's formula index — tiny, no tree walk).
fn fetch_formula_markers(agent: &ureq::Agent, base: &str) -> Option<HashSet<String>> {
    let body: Value = agent
        .get(&format!("{base}/formulas"))
        .set("Accept", "application/json")
        .call()
        .ok()?
        .into_json()
        .ok()?;
    parse_formula_listing(&body)
}

fn parse_formula_listing(body: &Value) -> Option<HashSet<String>> {
    Some(
        body["formulas"]
            .as_array()?
            .iter()
            .filter_map(|f| Some(f["path"].as_str()?.to_string()))
            .collect(),
    )
}

/// Subscribe forever: connect, stream frames, reconnect with Last-Event-ID.
/// After each snapshot, fetch the raw tree once to mark formula nodes.
fn feed_thread(base: String, tx: mpsc::Sender<Feed>, last_seen: Arc<AtomicU64>) {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(3))
        .build();
    let mut last_markers = std::time::Instant::now() - Duration::from_secs(10);
    loop {
        let mut req = agent
            .get(&format!("{base}/doc"))
            .set("Accept", "text/event-stream");
        let seen = last_seen.load(Ordering::Relaxed);
        if seen > 0 {
            req = req.set("Last-Event-ID", &seen.to_string());
        }
        let resp = match req.call() {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.send(Feed::Conn(format!("connect failed: {e}")));
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        let _ = tx.send(Feed::Conn("live".into()));
        let mut reader = resp.into_reader();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        'stream: loop {
            let n = match reader.read(&mut chunk) {
                Ok(0) | Err(_) => break 'stream,
                Ok(n) => n,
            };
            buf.extend_from_slice(&chunk[..n]);
            while let Some(pos) = buf.windows(2).position(|w| w == b"\n\n") {
                let frame = String::from_utf8_lossy(&buf[..pos]).into_owned();
                buf.drain(..pos + 2);
                let Some((name, id, data)) = parse_frame(&frame) else {
                    continue; // keep-alive comment
                };
                if let Some(v) = id {
                    last_seen.store(v, Ordering::Relaxed);
                }
                match name.as_str() {
                    "snapshot" => {
                        let _ = tx.send(Feed::Snapshot {
                            version: data["version"].as_u64().unwrap_or(0),
                            exists: data["exists"].as_bool().unwrap_or(false),
                            value: data["value"].clone(),
                            resync: data["resync"].as_bool().unwrap_or(false),
                        });
                        if let Some(set) = fetch_formula_markers(&agent, &base) {
                            let _ = tx.send(Feed::Formulas(set));
                        }
                        last_markers = std::time::Instant::now();
                    }
                    "change" => {
                        let _ = tx.send(Feed::Change {
                            version: data["version"].as_u64().unwrap_or(0),
                            patch: data["patch"].as_array().cloned().unwrap_or_default(),
                        });
                        // SSE carries plain values only, so a change may have
                        // created or deleted formulas invisibly; refresh the
                        // markers, debounced so busy streams stay cheap.
                        if last_markers.elapsed() > Duration::from_secs(1) {
                            if let Some(set) = fetch_formula_markers(&agent, &base) {
                                let _ = tx.send(Feed::Formulas(set));
                            }
                            last_markers = std::time::Instant::now();
                        }
                    }
                    _ => {}
                }
            }
        }
        let _ = tx.send(Feed::Conn("reconnecting…".into()));
        std::thread::sleep(Duration::from_millis(500));
    }
}

// ----------------------------------------------------------------- HTTP ops

/// GET the raw node (formulas visible) plus its ETag for a guarded write.
fn fetch_raw(agent: &ureq::Agent, base: &str, ptr: &str) -> Result<(String, Value), String> {
    let resp = agent
        .get(&format!("{}?raw=true", url_of(base, ptr)))
        .set("Accept", "application/json")
        .call()
        .map_err(|e| short_err(e))?;
    let etag = resp.header("etag").unwrap_or_default().to_string();
    let body = resp.into_json().map_err(|e| e.to_string())?;
    Ok((etag, body))
}

fn send_guarded(
    agent: &ureq::Agent,
    method: &str,
    url: &str,
    etag: Option<&str>,
    body: Option<&Value>,
) -> Result<(), String> {
    let mut req = agent.request(method, url);
    if let Some(t) = etag {
        // `?raw=true` reads carry a `-raw` variant tag; both parse to the
        // same version server-side, so pass it through untouched.
        req = req.set("If-Match", t);
    }
    let result = match body {
        Some(b) => req
            .set("Content-Type", "application/json")
            .send_string(&b.to_string()),
        None => req.call(),
    };
    result.map(|_| ()).map_err(|e| short_err(e))
}

fn short_err(e: ureq::Error) -> String {
    match e {
        ureq::Error::Status(412, _) => "conflict: changed since you loaded it (412)".into(),
        ureq::Error::Status(code, resp) => {
            let msg = resp.into_string().unwrap_or_default();
            let msg = msg.trim();
            if msg.is_empty() {
                format!("HTTP {code}")
            } else {
                format!("HTTP {code}: {}", &msg[..msg.len().min(120)])
            }
        }
        other => other.to_string(),
    }
}

/// `=expr` declares a formula; anything else is JSON; bare words that don't
/// parse become strings (spreadsheet-style convenience).
fn parse_input(buf: &str) -> Value {
    if let Some(expr) = buf.strip_prefix('=') {
        return json!({ "$formula": expr.trim() });
    }
    serde_json::from_str(buf).unwrap_or_else(|_| Value::String(buf.to_string()))
}

// ----------------------------------------------------------------- app state

struct Row {
    ptr: String,
    label: String,
    depth: usize,
    preview: String,
    expandable: bool,
    expanded: bool,
}

enum Mode {
    Browse,
    Input {
        title: String,
        buf: String,
        action: InputAction,
    },
    Confirm {
        prompt: String,
        ptr: String,
        etag: String,
    },
}

enum InputAction {
    Edit { ptr: String, etag: String },
    AddToObject { parent: String },
    AddToArray { parent: String },
}

struct App {
    base: String,
    agent: ureq::Agent,
    root: Value,
    exists: bool,
    version: u64,
    conn: String,
    formulas: HashSet<String>,
    expanded: HashSet<String>,
    rows: Vec<Row>,
    selected: usize,
    mode: Mode,
    status: String,
}

impl App {
    fn new(base: String) -> App {
        let mut expanded = HashSet::new();
        expanded.insert(String::new()); // root starts open
        App {
            base,
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(5))
                .build(),
            root: Value::Null,
            exists: false,
            version: 0,
            conn: "connecting…".into(),
            formulas: HashSet::new(),
            expanded,
            rows: Vec::new(),
            selected: 0,
            mode: Mode::Browse,
            status: String::new(),
        }
    }

    fn rebuild_rows(&mut self) {
        fn preview(v: &Value) -> String {
            match v {
                Value::Object(m) => format!("{{{}}}", m.len()),
                Value::Array(a) => format!("[{}]", a.len()),
                Value::String(s) => {
                    let s = s.chars().take(60).collect::<String>();
                    format!("\"{s}\"")
                }
                other => other.to_string(),
            }
        }
        fn walk(app: &App, v: &Value, ptr: &str, label: &str, depth: usize, out: &mut Vec<Row>) {
            let expandable = matches!(v, Value::Object(m) if !m.is_empty())
                || matches!(v, Value::Array(a) if !a.is_empty());
            let expanded = expandable && app.expanded.contains(ptr);
            out.push(Row {
                ptr: ptr.to_string(),
                label: label.to_string(),
                depth,
                preview: preview(v),
                expandable,
                expanded,
            });
            if !expanded {
                return;
            }
            match v {
                Value::Object(m) => {
                    for (k, child) in m {
                        let p = format!("{ptr}/{}", escape_token(k));
                        walk(app, child, &p, k, depth + 1, out);
                    }
                }
                Value::Array(a) => {
                    for (i, child) in a.iter().enumerate() {
                        let p = format!("{ptr}/{i}");
                        walk(app, child, &p, &i.to_string(), depth + 1, out);
                    }
                }
                _ => {}
            }
        }
        let mut rows = Vec::new();
        if self.exists {
            walk(self, &self.root.clone(), "", "/doc", 0, &mut rows);
        }
        self.rows = rows;
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
    }

    fn node_at(&self, ptr: &str) -> Option<&Value> {
        let mut cur = &self.root;
        for tok in parse_pointer(ptr) {
            cur = match cur {
                Value::Object(m) => m.get(&tok)?,
                Value::Array(a) => a.get(tok.parse::<usize>().ok()?)?,
                _ => return None,
            };
        }
        Some(cur)
    }

    fn on_feed(&mut self, feed: Feed) {
        match feed {
            Feed::Snapshot {
                version,
                exists,
                value,
                resync,
            } => {
                self.version = version;
                self.exists = exists;
                self.root = value;
                if resync {
                    self.status = "resynced".into();
                }
                self.rebuild_rows();
            }
            Feed::Change { version, patch } => {
                self.version = version;
                if apply_patch(&mut self.root, &mut self.exists, &patch).is_err() {
                    // Mirror out of step: mark stale; the next snapshot heals.
                    self.status = "patch mismatch; awaiting resync".into();
                }
                self.rebuild_rows();
            }
            Feed::Formulas(set) => self.formulas = set,
            Feed::Conn(s) => self.conn = s,
        }
    }
}

// ----------------------------------------------------------------------- UI

fn ui(f: &mut ratatui::Frame, app: &App, list_state: &mut ListState) {
    let [tree_area, detail_area, bar] = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(7),
        Constraint::Length(2),
    ])
    .areas(f.area());

    let items: Vec<ListItem> = app
        .rows
        .iter()
        .map(|r| {
            let fold = if !r.expandable {
                "  "
            } else if r.expanded {
                "▾ "
            } else {
                "▸ "
            };
            let marker = if app.formulas.contains(&r.ptr) { "ƒ " } else { "" };
            let line = Line::from(vec![
                Span::raw("  ".repeat(r.depth)),
                Span::raw(fold),
                Span::styled(marker, Style::default().fg(Color::Magenta)),
                Span::styled(r.label.clone(), Style::default().add_modifier(Modifier::BOLD)),
                Span::raw("  "),
                Span::styled(r.preview.clone(), Style::default().fg(Color::DarkGray)),
            ]);
            ListItem::new(line)
        })
        .collect();
    list_state.select(if app.rows.is_empty() {
        None
    } else {
        Some(app.selected)
    });
    let title = format!(" {} · v{} · {} ", app.base, app.version, app.conn);
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
    f.render_stateful_widget(list, tree_area, list_state);

    let detail = app
        .rows
        .get(app.selected)
        .map(|r| {
            let head = if app.formulas.contains(&r.ptr) {
                format!("{}  (ƒ formula — press e to see the expression)\n", r.ptr)
            } else {
                format!("{}\n", r.ptr)
            };
            let body = app
                .node_at(&r.ptr)
                .map(|v| serde_json::to_string_pretty(v).unwrap_or_default())
                .unwrap_or_default();
            format!("{head}{}", body.chars().take(2000).collect::<String>())
        })
        .unwrap_or_else(|| "no document — press a to create the root".into());
    f.render_widget(
        Paragraph::new(detail)
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title(" selected ")),
        detail_area,
    );

    let bottom = match &app.mode {
        Mode::Browse => format!(
            "{}  ·  ↑↓ move  ←→ fold  e edit  a add  d delete  q quit",
            app.status
        ),
        Mode::Input { title, buf, .. } => format!("{title}: {buf}▏   (Enter save · Esc cancel)"),
        Mode::Confirm { prompt, .. } => format!("{prompt} (y/n)"),
    };
    f.render_widget(Paragraph::new(bottom), bar);
}

// --------------------------------------------------------------- interaction

fn handle_key(app: &mut App, code: KeyCode) -> bool {
    match &mut app.mode {
        Mode::Browse => match code {
            KeyCode::Char('q') => return true,
            KeyCode::Up | KeyCode::Char('k') => app.selected = app.selected.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                app.selected = (app.selected + 1).min(app.rows.len().saturating_sub(1))
            }
            KeyCode::Char('g') => app.selected = 0,
            KeyCode::Char('G') => app.selected = app.rows.len().saturating_sub(1),
            KeyCode::Right | KeyCode::Char('l') | KeyCode::Enter => {
                if let Some(r) = app.rows.get(app.selected) {
                    if r.expandable {
                        let ptr = r.ptr.clone();
                        if !app.expanded.insert(ptr.clone()) {
                            app.expanded.remove(&ptr);
                        }
                        app.rebuild_rows();
                    }
                }
            }
            KeyCode::Left | KeyCode::Char('h') => {
                if let Some(r) = app.rows.get(app.selected) {
                    if r.expanded {
                        app.expanded.remove(&r.ptr.clone());
                    } else if let Some(cut) = r.ptr.rfind('/') {
                        // jump to parent
                        let parent = r.ptr[..cut].to_string();
                        if let Some(i) = app.rows.iter().position(|x| x.ptr == parent) {
                            app.selected = i;
                        }
                    }
                    app.rebuild_rows();
                }
            }
            KeyCode::Char('e') => {
                let Some(r) = app.rows.get(app.selected) else {
                    // No document yet: create the root.
                    app.mode = Mode::Input {
                        title: "new /doc value".into(),
                        buf: String::new(),
                        action: InputAction::Edit {
                            ptr: String::new(),
                            etag: "*".into(),
                        },
                    };
                    return false;
                };
                let ptr = r.ptr.clone();
                match fetch_raw(&app.agent, &app.base, &ptr) {
                    Ok((etag, raw)) => {
                        let buf = match raw.get("$formula").and_then(|f| f.as_str()) {
                            Some(expr) => format!("={expr}"),
                            None => raw.to_string(),
                        };
                        app.mode = Mode::Input {
                            title: format!("edit {}", if ptr.is_empty() { "/doc" } else { &ptr }),
                            buf,
                            action: InputAction::Edit { ptr, etag },
                        };
                    }
                    Err(e) => app.status = e,
                }
            }
            KeyCode::Char('a') => {
                let (parent, is_array) = match app.rows.get(app.selected) {
                    Some(r) => match app.node_at(&r.ptr) {
                        Some(Value::Array(_)) => (r.ptr.clone(), true),
                        Some(Value::Object(_)) => (r.ptr.clone(), false),
                        _ => {
                            app.status = "select an object or array to add into".into();
                            return false;
                        }
                    },
                    None => (String::new(), false), // no doc: create root key
                };
                app.mode = if is_array {
                    Mode::Input {
                        title: format!("append to {parent}"),
                        buf: String::new(),
                        action: InputAction::AddToArray { parent },
                    }
                } else {
                    Mode::Input {
                        title: format!("add key = value under {}", if parent.is_empty() { "/doc" } else { &parent }),
                        buf: String::new(),
                        action: InputAction::AddToObject { parent },
                    }
                };
            }
            KeyCode::Char('d') => {
                if let Some(r) = app.rows.get(app.selected) {
                    let ptr = r.ptr.clone();
                    match fetch_raw(&app.agent, &app.base, &ptr) {
                        Ok((etag, _)) => {
                            app.mode = Mode::Confirm {
                                prompt: format!("delete {}?", if ptr.is_empty() { "/doc" } else { &ptr }),
                                ptr,
                                etag,
                            }
                        }
                        Err(e) => app.status = e,
                    }
                }
            }
            _ => {}
        },
        Mode::Input { buf, .. } => match code {
            KeyCode::Esc => app.mode = Mode::Browse,
            KeyCode::Backspace => {
                buf.pop();
            }
            KeyCode::Char(c) => buf.push(c),
            KeyCode::Enter => {
                let Mode::Input { buf, action, .. } = std::mem::replace(&mut app.mode, Mode::Browse)
                else {
                    unreachable!()
                };
                let result = match action {
                    InputAction::Edit { ptr, etag } => {
                        let body = parse_input(buf.trim());
                        send_guarded(
                            &app.agent,
                            "PUT",
                            &url_of(&app.base, &ptr),
                            (etag != "*").then_some(etag.as_str()),
                            Some(&body),
                        )
                    }
                    InputAction::AddToArray { parent } => send_guarded(
                        &app.agent,
                        "POST",
                        &url_of(&app.base, &parent),
                        None,
                        Some(&parse_input(buf.trim())),
                    ),
                    InputAction::AddToObject { parent } => match buf.split_once('=') {
                        Some((key, value)) if !key.trim().is_empty() => {
                            let ptr =
                                format!("{parent}/{}", escape_token(key.trim()));
                            send_guarded(
                                &app.agent,
                                "PUT",
                                &url_of(&app.base, &ptr),
                                None,
                                Some(&parse_input(value.trim())),
                            )
                        }
                        _ => Err("expected `key = value`".into()),
                    },
                };
                app.status = match result {
                    Ok(()) => "saved".into(),
                    Err(e) => e,
                };
            }
            _ => {}
        },
        Mode::Confirm { .. } => match code {
            KeyCode::Char('y') => {
                let Mode::Confirm { ptr, etag, .. } = std::mem::replace(&mut app.mode, Mode::Browse)
                else {
                    unreachable!()
                };
                app.status = match send_guarded(
                    &app.agent,
                    "DELETE",
                    &url_of(&app.base, &ptr),
                    Some(&etag),
                    None,
                ) {
                    Ok(()) => "deleted".into(),
                    Err(e) => e,
                };
            }
            _ => app.mode = Mode::Browse,
        },
    }
    false
}

// --------------------------------------------------------------------- main

fn main() {
    let mut base = "http://127.0.0.1:8080".to_string();
    let mut check = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--check" => check = true,
            "--help" | "-h" => {
                println!("usage: visijson-tui [URL] [--check]");
                return;
            }
            other => base = other.trim_end_matches('/').to_string(),
        }
    }

    let (tx, rx) = mpsc::channel();
    let last_seen = Arc::new(AtomicU64::new(0));
    {
        let (base, tx, last_seen) = (base.clone(), tx.clone(), last_seen.clone());
        std::thread::spawn(move || feed_thread(base, tx, last_seen));
    }

    if check {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
                Ok(Feed::Snapshot { version, exists, value, .. }) => {
                    let keys = match &value {
                        Value::Object(m) => m.keys().cloned().collect::<Vec<_>>().join(", "),
                        other => other.to_string().chars().take(80).collect(),
                    };
                    println!("ok: version={version} exists={exists} keys=[{keys}]");
                    return;
                }
                Ok(Feed::Conn(s)) if s.starts_with("connect failed") => {
                    eprintln!("{s}");
                    std::process::exit(1);
                }
                Ok(_) => continue,
                Err(_) => {
                    eprintln!("no snapshot within 5s");
                    std::process::exit(1);
                }
            }
        }
    }

    let mut app = App::new(base);
    let mut terminal = ratatui::init();
    let mut list_state = ListState::default();
    loop {
        while let Ok(feed) = rx.try_recv() {
            app.on_feed(feed);
        }
        if terminal.draw(|f| ui(f, &app, &mut list_state)).is_err() {
            break;
        }
        match event::poll(Duration::from_millis(80)) {
            Ok(true) => {
                if let Ok(Event::Key(k)) = event::read() {
                    if k.kind == KeyEventKind::Press && handle_key(&mut app, k.code) {
                        break;
                    }
                }
            }
            Ok(false) => {}
            Err(_) => break,
        }
    }
    ratatui::restore();
}

// -------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_apply_add_replace_remove() {
        let mut v = json!({"a": {"b": 1}, "arr": [1, 2]});
        let mut exists = true;
        apply_patch(
            &mut v,
            &mut exists,
            &[
                json!({"op": "replace", "path": "/a/b", "value": 2}),
                json!({"op": "add", "path": "/a/c", "value": true}),
                json!({"op": "add", "path": "/arr/1", "value": 9}),
                json!({"op": "remove", "path": "/arr/0"}),
            ],
        )
        .unwrap();
        assert_eq!(v, json!({"a": {"b": 2, "c": true}, "arr": [9, 2]}));

        apply_patch(&mut v, &mut exists, &[json!({"op": "remove", "path": ""})]).unwrap();
        assert!(!exists);
    }

    #[test]
    fn pointer_escaping_roundtrips() {
        assert_eq!(escape_token("a/b~c"), "a~1b~0c");
        assert_eq!(parse_pointer("/a~1b~0c/2"), vec!["a/b~c", "2"]);
        assert!(url_of("http://x", "/a b/ü").contains("/doc/a%20b/%C3%BC"));
    }

    #[test]
    fn sse_frames_parse() {
        let (name, id, data) =
            parse_frame("event: change\nid: 7\ndata: {\"patch\":[]}").unwrap();
        assert_eq!((name.as_str(), id), ("change", Some(7)));
        assert_eq!(data["patch"], json!([]));
        assert!(parse_frame(": keep-alive").is_none());
    }

    #[test]
    fn input_parsing_is_spreadsheet_flavored() {
        assert_eq!(parse_input("= /a + 1"), json!({"$formula": "/a + 1"}));
        assert_eq!(parse_input("42"), json!(42));
        assert_eq!(parse_input("{\"x\": 1}"), json!({"x": 1}));
        assert_eq!(parse_input("hello world"), json!("hello world"));
    }

    #[test]
    fn formula_markers_from_listing() {
        let body = json!({
            "version": 9,
            "formulas": [
                {"path": "/a", "expr": "/x + 1"},
                {"path": "/nest/b", "expr": "sum(/xs)"},
            ],
        });
        assert_eq!(
            parse_formula_listing(&body).unwrap(),
            HashSet::from(["/a".to_string(), "/nest/b".to_string()])
        );
    }
}
