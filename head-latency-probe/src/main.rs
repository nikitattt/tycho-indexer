//! Minimal head-latency probe for comparing Substreams near-head delivery against RPC
//! websocket `eth_subscribe` `newHeads`.
//!
//! Examples:
//!
//! ```bash
//! cargo run -p head-latency-probe -- \
//!   --mode substreams \
//!   --substreams-endpoint "$SUBSTREAMS_ENDPOINT" \
//!   --substreams-api-token "$SUBSTREAMS_API_TOKEN" \
//!   --spkg ./path/to/package.spkg \
//!   --module map_blocks \
//!   --samples 100
//!
//! cargo run -p head-latency-probe -- \
//!   --mode rpc-ws \
//!   --rpc-ws-url "$RPC_WS_URL" \
//!   --samples 100
//!
//! cargo run -p head-latency-probe -- \
//!   --mode compare \
//!   --env-file ./probe.env \
//!   --spkg ./path/to/package.spkg \
//!   --module map_blocks \
//!   --samples 100
//! ```

mod pb;
mod substreams;

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context};
use chrono::{Local, TimeZone};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde_json::{json, Value};
use tokio::{
    sync::{mpsc as tokio_mpsc, watch},
    task::JoinHandle,
    time,
};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

use crate::{
    pb::sf::substreams::{
        rpc::{
            v2::{response::Message as SubstreamsMessage, BlockScopedData},
            v3::Request,
        },
        v1::Package,
    },
    substreams::SubstreamsEndpoint,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Substreams,
    RpcWs,
    Compare,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, env = "ENV_FILE")]
    env_file: Option<PathBuf>,

    #[arg(long, env = "MODE", value_enum, default_value = "compare")]
    mode: Mode,

    #[arg(long, env = "SUBSTREAMS_ENDPOINT")]
    substreams_endpoint: Option<String>,

    #[arg(long, env = "SUBSTREAMS_API_TOKEN")]
    substreams_api_token: Option<String>,

    #[arg(long, env = "SUBSTREAMS_NETWORK")]
    substreams_network: Option<String>,

    #[arg(long, env = "SUBSTREAMS_SPKG", value_delimiter = ',', num_args = 1..)]
    spkg: Vec<PathBuf>,

    #[arg(long, env = "SUBSTREAMS_MODULE")]
    module: Option<String>,

    #[arg(long = "substreams-param", env = "SUBSTREAMS_PARAMS", value_delimiter = ',')]
    substreams_params: Vec<String>,

    #[arg(long, env = "RPC_WS_URL")]
    rpc_ws_url: Option<String>,

    #[arg(long, env = "SAMPLES", default_value_t = 100)]
    samples: usize,

    #[arg(long, env = "START_BLOCK_OFFSET", default_value_t = -3)]
    start_block_offset: i64,

    #[arg(long, env = "PARTIAL_BLOCKS", default_value_t = false)]
    partial_blocks: bool,

    #[arg(long, env = "SUBSTREAMS_PRODUCTION_MODE", default_value_t = false)]
    substreams_production_mode: bool,

    #[arg(long, env = "SKIP_BLOCKS", default_value_t = 0)]
    skip_blocks: usize,

    #[arg(long, env = "USE_BLOCK_TIMESTAMPS", default_value_t = false)]
    use_block_timestamps: bool,

    #[arg(long, env = "FOLLOW_FOREVER", default_value_t = false)]
    follow_forever: bool,

    #[arg(long, env = "LIVE_PER_BLOCK", default_value_t = false)]
    live_per_block: bool,

    #[arg(long, env = "CSV_PATH", default_value = "head-latency-probe.csv")]
    csv_path: PathBuf,

    #[arg(long, env = "SUMMARY_EVERY", default_value_t = 20)]
    summary_every: usize,

    #[arg(long, env = "SUMMARY_EVERY_LONG", default_value_t = 200)]
    summary_every_long: usize,

    #[arg(long, env = "TIMEOUT_SECS", default_value_t = 120)]
    timeout_secs: u64,
}

#[derive(Clone, Debug)]
enum SourceKind {
    Rpc,
    Substreams,
}

#[derive(Clone, Debug)]
struct ProbeEvent {
    source: String,
    source_kind: SourceKind,
    event_kind: &'static str,
    block_number: u64,
    block_hash: Option<String>,
    block_time_ms: Option<i64>,
    receive_time_ms: i64,
    lag_ms: Option<i64>,
    is_partial: bool,
    is_last_partial: bool,
}

#[derive(Default)]
struct StatsAccumulator {
    values: Vec<i64>,
    sum: i128,
    min: Option<i64>,
    max: Option<i64>,
    latest: Option<i64>,
}

impl StatsAccumulator {
    fn record(&mut self, value: i64) {
        self.values.push(value);
        self.sum += value as i128;
        self.min = Some(
            self.min
                .map_or(value, |current| current.min(value)),
        );
        self.max = Some(
            self.max
                .map_or(value, |current| current.max(value)),
        );
        self.latest = Some(value);
    }

    fn summary(&self, name: String) -> Option<StatsSummary> {
        stats_summary(name, &self.values)
    }
}

fn stats_summary(name: String, values: &[i64]) -> Option<StatsSummary> {
    if values.is_empty() {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    let sum: i128 = values
        .iter()
        .map(|value| *value as i128)
        .sum();

    Some(StatsSummary {
        name,
        count: sorted.len(),
        min: *sorted.first()?,
        max: *sorted.last()?,
        mean: sum as f64 / sorted.len() as f64,
        p50: percentile(&sorted, 0.50),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        latest: *values.last()?,
    })
}

struct StatsSummary {
    name: String,
    count: usize,
    min: i64,
    max: i64,
    mean: f64,
    p50: i64,
    p95: i64,
    p99: i64,
    latest: i64,
}

#[derive(Clone)]
struct CompareObservation {
    receive_time_ms: i64,
    block_hash: Option<String>,
    block_time_ms: Option<i64>,
    event_kind: &'static str,
    is_partial: bool,
    is_last_partial: bool,
}

#[derive(Default)]
struct CompareState {
    rpc_receive_time_ms: Option<i64>,
    substreams: HashMap<String, CompareObservation>,
}

struct Aggregator {
    raw_events: usize,
    summary_every: usize,
    summary_every_long: usize,
    compare_enabled: bool,
    use_block_timestamps: bool,
    live_per_block: bool,
    skip_blocks: usize,
    csv_tx: mpsc::Sender<CsvMessage>,
    lag_stats: BTreeMap<String, StatsAccumulator>,
    compare_stats: BTreeMap<String, StatsAccumulator>,
    compare_blocks: HashMap<u64, CompareState>,
    seen_blocks_per_metric: HashMap<String, HashSet<u64>>,
    last_printed_counts: HashMap<String, usize>,
    recent_compare_values: HashMap<String, VecDeque<i64>>,
}

impl Aggregator {
    fn new(
        summary_every: usize,
        summary_every_long: usize,
        compare_enabled: bool,
        use_block_timestamps: bool,
        live_per_block: bool,
        skip_blocks: usize,
        csv_tx: mpsc::Sender<CsvMessage>,
    ) -> Self {
        Self {
            raw_events: 0,
            summary_every,
            summary_every_long,
            compare_enabled,
            use_block_timestamps,
            live_per_block,
            skip_blocks,
            csv_tx,
            lag_stats: BTreeMap::new(),
            compare_stats: BTreeMap::new(),
            compare_blocks: HashMap::new(),
            seen_blocks_per_metric: HashMap::new(),
            last_printed_counts: HashMap::new(),
            recent_compare_values: HashMap::new(),
        }
    }

    fn compare_window_limit(&self) -> usize {
        self.summary_every
            .max(self.summary_every_long)
            .max(1)
    }

    fn rolling_summary_key(metric_key: &str, window_size: usize) -> String {
        format!("{metric_key}|window={window_size}")
    }

    fn maybe_print_compare_window_summary(&mut self, compare_key: &str, window_size: usize) {
        if window_size == 0 {
            return;
        }

        let current_count = self
            .compare_stats
            .get(compare_key)
            .map(|stats| stats.values.len())
            .unwrap_or_default();
        let last_printed = self
            .last_printed_counts
            .get(&Self::rolling_summary_key(compare_key, window_size))
            .copied()
            .unwrap_or_default();

        if current_count < window_size || current_count / window_size <= last_printed / window_size
        {
            return;
        }

        if let Some(summary) = self
            .recent_compare_values
            .get(compare_key)
            .and_then(|values| {
                let window_values = values
                    .iter()
                    .rev()
                    .take(window_size)
                    .copied()
                    .collect::<Vec<_>>();
                let window_values = window_values
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>();
                stats_summary(format!("{compare_key} window={window_size}"), &window_values)
            })
        {
            println!(
                "summary {} n={} min={} p50={} p95={} p99={} max={} latest={}",
                summary.name,
                summary.count,
                fmt_ms(summary.min),
                fmt_ms(summary.p50),
                fmt_ms(summary.p95),
                fmt_ms(summary.p99),
                fmt_ms(summary.max),
                fmt_ms(summary.latest),
            );
        }

        self.last_printed_counts
            .insert(Self::rolling_summary_key(compare_key, window_size), current_count);
    }

    fn handle_event(&mut self, event: ProbeEvent) -> anyhow::Result<()> {
        self.raw_events += 1;
        self.write_raw_row(&event)?;

        if self.live_per_block && !self.compare_enabled {
            println!(
                "{} - {} - {}",
                event.block_number,
                event.source,
                fmt_clock_ms(event.receive_time_ms)
            );
        }

        if self.use_block_timestamps {
            if let Some(lag_ms) = event.lag_ms {
                for metric_key in lag_metric_keys(&event) {
                    if self.should_record_metric(&metric_key, event.block_number) {
                        self.lag_stats
                            .entry(metric_key)
                            .or_default()
                            .record(lag_ms);
                    }
                }
            }
        }

        if !self.compare_enabled {
            return Ok(());
        }

        match event.source_kind {
            SourceKind::Rpc => {
                let observations = {
                    let compare_state = self
                        .compare_blocks
                        .entry(event.block_number)
                        .or_default();
                    compare_state.rpc_receive_time_ms = Some(event.receive_time_ms);
                    compare_state.substreams.clone()
                };

                for (metric_key, observation) in observations {
                    self.record_compare_row(
                        &metric_key,
                        event.block_number,
                        observation.block_hash,
                        observation.block_time_ms,
                        observation.receive_time_ms,
                        observation.event_kind,
                        observation.is_partial,
                        observation.is_last_partial,
                        event.receive_time_ms,
                    )?;
                }
            }
            SourceKind::Substreams => {
                let metric_keys = lag_metric_keys(&event);
                let rpc_receive_time_ms = {
                    let compare_state = self
                        .compare_blocks
                        .entry(event.block_number)
                        .or_default();
                    for metric_key in &metric_keys {
                        compare_state
                            .substreams
                            .entry(metric_key.clone())
                            .or_insert_with(|| CompareObservation {
                                receive_time_ms: event.receive_time_ms,
                                block_hash: event.block_hash.clone(),
                                block_time_ms: event.block_time_ms,
                                event_kind: event.event_kind,
                                is_partial: event.is_partial,
                                is_last_partial: event.is_last_partial,
                            });
                    }
                    compare_state.rpc_receive_time_ms
                };

                if let Some(rpc_receive_time_ms) = rpc_receive_time_ms {
                    for metric_key in metric_keys {
                        self.record_compare_row(
                            &metric_key,
                            event.block_number,
                            event.block_hash.clone(),
                            event.block_time_ms,
                            event.receive_time_ms,
                            event.event_kind,
                            event.is_partial,
                            event.is_last_partial,
                            rpc_receive_time_ms,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    fn should_record_metric(&mut self, metric_key: &str, block_number: u64) -> bool {
        let seen = self
            .seen_blocks_per_metric
            .entry(metric_key.to_string())
            .or_default();
        seen.insert(block_number);
        seen.len() > self.skip_blocks
    }

    fn write_raw_row(&self, event: &ProbeEvent) -> anyhow::Result<()> {
        self.csv_tx
            .send(CsvMessage::Line(csv_line([
                event.source.clone(),
                event.event_kind.to_string(),
                event.block_number.to_string(),
                event
                    .block_hash
                    .clone()
                    .unwrap_or_default(),
                opt_i64(event.block_time_ms),
                event.receive_time_ms.to_string(),
                opt_i64(event.lag_ms),
                bool_cell(event.is_partial),
                bool_cell(event.is_last_partial),
                String::new(),
                String::new(),
                String::new(),
            ])))
            .context("send raw csv row")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_compare_row(
        &mut self,
        metric_key: &str,
        block_number: u64,
        block_hash: Option<String>,
        block_time_ms: Option<i64>,
        receive_time_ms: i64,
        event_kind: &'static str,
        is_partial: bool,
        is_last_partial: bool,
        rpc_receive_time_ms: i64,
    ) -> anyhow::Result<()> {
        let delta_vs_rpc_ms = receive_time_ms - rpc_receive_time_ms;
        let compare_key = format!("compare:{metric_key}");

        if self.should_record_metric(&compare_key, block_number) {
            self.compare_stats
                .entry(compare_key.clone())
                .or_default()
                .record(delta_vs_rpc_ms);

            let compare_window_limit = self.compare_window_limit();
            let recent_values = self
                .recent_compare_values
                .entry(compare_key.clone())
                .or_default();
            recent_values.push_back(delta_vs_rpc_ms);
            while recent_values.len() > compare_window_limit {
                recent_values.pop_front();
            }

            self.maybe_print_compare_window_summary(&compare_key, self.summary_every);
            self.maybe_print_compare_window_summary(&compare_key, self.summary_every_long);
        }

        if self.live_per_block {
            println!(
                "{} - {}ms (rpc={} substreams={})",
                block_number,
                delta_vs_rpc_ms,
                fmt_clock_ms(rpc_receive_time_ms),
                fmt_clock_ms(receive_time_ms)
            );
        }

        self.csv_tx
            .send(CsvMessage::Line(csv_line([
                compare_key,
                format!("{event_kind}_vs_rpc"),
                block_number.to_string(),
                block_hash.unwrap_or_default(),
                opt_i64(block_time_ms),
                receive_time_ms.to_string(),
                opt_i64(
                    self.use_block_timestamps
                        .then_some(block_time_ms)
                        .flatten()
                        .map(|ts| receive_time_ms - ts),
                ),
                bool_cell(is_partial),
                bool_cell(is_last_partial),
                rpc_receive_time_ms.to_string(),
                delta_vs_rpc_ms.to_string(),
                String::new(),
            ])))
            .context("send compare csv row")?;
        Ok(())
    }

    fn print_live(&self) {
        let mut parts = Vec::new();

        if self.use_block_timestamps {
            for summary in self.lag_summaries() {
                parts.push(format!(
                    "{} n={} p50={} latest={}",
                    summary.name,
                    summary.count,
                    fmt_ms(summary.p50),
                    fmt_ms(summary.latest),
                ));
            }
        }

        for summary in self.compare_summaries() {
            parts.push(format!(
                "{} n={} p50={} latest={}",
                summary.name,
                summary.count,
                fmt_ms(summary.p50),
                fmt_ms(summary.latest),
            ));
        }

        println!("events={} {}", self.raw_events, parts.join(" | "));
    }

    fn print_final(&self) {
        println!();
        println!("Final summary");
        println!("skip_blocks={}", self.skip_blocks);
        if self.use_block_timestamps {
            self.print_table(
                "Lag vs block time (coarse, second-resolution timestamps)",
                self.lag_summaries(),
            );
        } else {
            println!("Lag vs block time");
            println!(
                "  disabled; set USE_BLOCK_TIMESTAMPS=true to enable coarse header-timestamp lag"
            );
        }
        if self.compare_enabled {
            self.print_table("Substreams vs rpc-ws receive-time delta", self.compare_summaries());
        }
    }

    fn print_table(&self, title: &str, rows: Vec<StatsSummary>) {
        println!("{title}");
        if rows.is_empty() {
            println!("  no samples recorded");
            return;
        }

        println!(
            "{:<42} {:>7} {:>8} {:>8} {:>9} {:>8} {:>8} {:>8} {:>8}",
            "metric", "count", "min", "max", "mean", "p50", "p95", "p99", "latest"
        );

        for row in rows {
            println!(
                "{:<42} {:>7} {:>8} {:>8} {:>9.1} {:>8} {:>8} {:>8} {:>8}",
                row.name,
                row.count,
                row.min,
                row.max,
                row.mean,
                row.p50,
                row.p95,
                row.p99,
                row.latest,
            );
        }
    }

    fn lag_summaries(&self) -> Vec<StatsSummary> {
        self.lag_stats
            .iter()
            .filter_map(|(name, stats)| stats.summary(name.clone()))
            .collect()
    }

    fn compare_summaries(&self) -> Vec<StatsSummary> {
        self.compare_stats
            .iter()
            .filter_map(|(name, stats)| stats.summary(name.clone()))
            .collect()
    }
}

enum CsvMessage {
    Line(String),
    Shutdown,
}

struct TaskDone {
    name: String,
    result: anyhow::Result<()>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env_file_from_args()?;
    let args = Args::parse();
    validate_args(&args)?;

    let (csv_tx, writer_handle) = spawn_csv_writer(&args.csv_path)?;
    let (event_tx, mut event_rx) = tokio_mpsc::unbounded_channel();
    let (done_tx, mut done_rx) = tokio_mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = watch::channel(false);

    let mut handles = Vec::new();
    let mut active_tasks = 0usize;

    if matches!(args.mode, Mode::Substreams | Mode::Compare) {
        let substreams_endpoint = args
            .substreams_endpoint
            .clone()
            .expect("validated substreams endpoint");
        let module = args
            .module
            .clone()
            .expect("validated module");
        let token = args.substreams_api_token.clone();

        for (spkg_path, source_name) in labeled_spkgs(&args.spkg) {
            active_tasks += 1;
            spawn_task(
                &mut handles,
                done_tx.clone(),
                source_name.clone(),
                run_substreams_source(
                    source_name,
                    spkg_path,
                    substreams_endpoint.clone(),
                    token.clone(),
                    args.substreams_network.clone(),
                    args.substreams_params.clone(),
                    module.clone(),
                    args.start_block_offset,
                    args.partial_blocks,
                    args.substreams_production_mode,
                    args.use_block_timestamps,
                    (!args.follow_forever).then_some(args.samples),
                    stop_rx.clone(),
                    event_tx.clone(),
                ),
            );
        }
    }

    if matches!(args.mode, Mode::RpcWs | Mode::Compare) {
        active_tasks += 1;
        spawn_task(
            &mut handles,
            done_tx.clone(),
            "rpc-ws".to_string(),
            run_rpc_ws_source(
                args.rpc_ws_url
                    .clone()
                    .expect("validated rpc ws url"),
                args.use_block_timestamps,
                (!args.follow_forever).then_some(args.samples),
                stop_rx.clone(),
                event_tx.clone(),
            ),
        );
    }

    drop(done_tx);
    drop(event_tx);

    let mut aggregator = Aggregator::new(
        args.summary_every,
        args.summary_every_long,
        matches!(args.mode, Mode::Compare),
        args.use_block_timestamps,
        args.live_per_block,
        args.skip_blocks,
        csv_tx.clone(),
    );
    let timeout = time::sleep(if args.follow_forever {
        Duration::from_secs(10 * 365 * 24 * 60 * 60)
    } else {
        Duration::from_secs(args.timeout_secs)
    });
    tokio::pin!(timeout);

    let mut failure = None;
    let mut timed_out = false;
    let mut interrupted = false;

    while active_tasks > 0 {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                interrupted = true;
                break;
            }
            Some(event) = event_rx.recv() => {
                aggregator.handle_event(event)?;
                if !args.live_per_block && aggregator.summary_every > 0 && aggregator.raw_events % aggregator.summary_every == 0 {
                    aggregator.print_live();
                }
            }
            Some(done) = done_rx.recv() => {
                active_tasks = active_tasks.saturating_sub(1);
                if let Err(err) = done.result {
                    failure = Some(err.context(format!("{} failed", done.name)));
                    break;
                }
            }
            _ = &mut timeout => {
                timed_out = true;
                break;
            }
        }
    }

    if failure.is_some() || timed_out {
        let _ = stop_tx.send(true);
        for handle in &handles {
            handle.abort();
        }
    }

    if interrupted {
        let _ = stop_tx.send(true);
        for handle in &handles {
            handle.abort();
        }
    }

    while let Ok(event) = event_rx.try_recv() {
        aggregator.handle_event(event)?;
    }

    if timed_out {
        println!("Timeout after {}s", args.timeout_secs);
    }
    if interrupted {
        println!("Interrupted");
    }

    aggregator.print_final();

    let _ = csv_tx.send(CsvMessage::Shutdown);
    match writer_handle.join() {
        Ok(result) => result?,
        Err(_) => bail!("csv writer thread panicked"),
    }

    if let Some(err) = failure {
        return Err(err);
    }

    Ok(())
}

fn spawn_task<F>(
    handles: &mut Vec<JoinHandle<()>>,
    done_tx: tokio_mpsc::UnboundedSender<TaskDone>,
    name: String,
    future: F,
) where
    F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    handles.push(tokio::spawn(async move {
        let result = future.await;
        let _ = done_tx.send(TaskDone { name, result });
    }));
}

fn validate_args(args: &Args) -> anyhow::Result<()> {
    if args.samples == 0 {
        bail!("--samples must be greater than 0");
    }

    match args.mode {
        Mode::Substreams | Mode::Compare => {
            if args.substreams_endpoint.is_none() {
                bail!("--substreams-endpoint or SUBSTREAMS_ENDPOINT is required");
            }
            if args.spkg.is_empty() {
                bail!("--spkg or SUBSTREAMS_SPKG is required");
            }
            if args.module.is_none() {
                bail!("--module or SUBSTREAMS_MODULE is required");
            }
        }
        Mode::RpcWs => {}
    }

    match args.mode {
        Mode::RpcWs | Mode::Compare => {
            if args.rpc_ws_url.is_none() {
                bail!("--rpc-ws-url or RPC_WS_URL is required");
            }
        }
        Mode::Substreams => {}
    }

    Ok(())
}

fn load_env_file_from_args() -> anyhow::Result<()> {
    let args: Vec<_> = env::args_os().collect();

    let env_file = args
        .iter()
        .skip(1)
        .find_map(|arg| {
            let arg = arg.to_string_lossy();
            arg.strip_prefix("--env-file=")
                .map(PathBuf::from)
        })
        .or_else(|| {
            let mut iter = args.iter().skip(1);
            while let Some(arg) = iter.next() {
                if arg == "--env-file" {
                    return iter.next().map(PathBuf::from);
                }
            }
            None
        })
        .or_else(|| env::var_os("ENV_FILE").map(PathBuf::from));

    if let Some(path) = env_file {
        load_env_file(&path)?;
    }

    Ok(())
}

fn load_env_file(path: &Path) -> anyhow::Result<()> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read env file {}", path.display()))?;

    for (line_number, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = line
            .strip_prefix("export ")
            .unwrap_or(line);
        let (key, value) = line.split_once('=').ok_or_else(|| {
            anyhow!(
                "invalid env file line {} in {}: expected KEY=VALUE",
                line_number + 1,
                path.display()
            )
        })?;

        let key = key.trim();
        if key.is_empty() {
            bail!("invalid env file line {} in {}: empty key", line_number + 1, path.display());
        }

        if env::var_os(key).is_some() {
            continue;
        }

        env::set_var(key, parse_env_value(value.trim()));
    }

    Ok(())
}

fn parse_env_value(value: &str) -> String {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return value[1..value.len() - 1].to_string();
        }
    }

    value.to_string()
}

struct SubstreamsRequestConfig {
    network: String,
    params: HashMap<String, String>,
}

fn build_substreams_request_config(
    package: &Package,
    network_override: Option<String>,
    param_overrides: &[String],
    module_name: &str,
) -> anyhow::Result<SubstreamsRequestConfig> {
    let network = network_override.unwrap_or_else(|| {
        if !package.network.is_empty() {
            return package.network.clone();
        }

        if package.networks.len() == 1 {
            return package
                .networks
                .keys()
                .next()
                .cloned()
                .unwrap_or_default();
        }

        String::new()
    });
    let mut params = package
        .networks
        .get(&network)
        .map(|network_params| network_params.params.clone())
        .unwrap_or_default();

    for override_entry in param_overrides {
        let (key, value) = override_entry
            .split_once('=')
            .ok_or_else(|| {
                anyhow!("invalid --substreams-param '{}': expected KEY=VALUE", override_entry)
            })?;
        params.insert(key.trim().to_string(), value.trim().to_string());
    }

    warn_if_missing_module_params(package, module_name, &params, &network);

    Ok(SubstreamsRequestConfig { network, params })
}

fn warn_if_missing_module_params(
    package: &Package,
    module_name: &str,
    params: &HashMap<String, String>,
    network: &str,
) {
    let requires_params = package
        .modules
        .as_ref()
        .and_then(|modules| {
            modules
                .modules
                .iter()
                .find(|module| module.name == module_name)
        })
        .map(|module| {
            module.inputs.iter().any(|input| {
                matches!(
                    input.input,
                    Some(crate::pb::sf::substreams::v1::module::input::Input::Params(_))
                )
            })
        })
        .unwrap_or(false);

    if requires_params && !params.contains_key(module_name) {
        println!(
            "warning: module {} requires params but none were found for network {}. Set --substreams-param {}=... or SUBSTREAMS_PARAMS",
            module_name, network, module_name
        );
    }
}

fn labeled_spkgs(spkgs: &[PathBuf]) -> Vec<(PathBuf, String)> {
    let mut seen = HashMap::<String, usize>::new();

    spkgs
        .iter()
        .map(|path| {
            let base = path
                .file_stem()
                .or_else(|| path.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("spkg")
                .to_string();

            let entry = seen.entry(base.clone()).or_default();
            *entry += 1;

            let label = if *entry == 1 {
                format!("substreams:{base}")
            } else {
                format!("substreams:{base}#{entry}")
            };

            (path.clone(), label)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn run_substreams_source(
    source_name: String,
    spkg_path: PathBuf,
    endpoint_url: String,
    api_token: Option<String>,
    network_override: Option<String>,
    param_overrides: Vec<String>,
    module: String,
    start_block_offset: i64,
    partial_blocks: bool,
    production_mode: bool,
    use_block_timestamps: bool,
    stop_after_samples: Option<usize>,
    mut stop_rx: watch::Receiver<bool>,
    event_tx: tokio_mpsc::UnboundedSender<ProbeEvent>,
) -> anyhow::Result<()> {
    let content = std::fs::read(&spkg_path)
        .with_context(|| format!("read spkg from {}", spkg_path.display()))?;
    let package = Package::decode(content.as_slice())
        .with_context(|| format!("decode spkg from {}", spkg_path.display()))?;
    let request_config =
        build_substreams_request_config(&package, network_override, &param_overrides, &module)?;
    let endpoint = std::sync::Arc::new(SubstreamsEndpoint::new(endpoint_url, api_token).await?);

    let param_modules = if request_config.params.is_empty() {
        "none".to_string()
    } else {
        request_config
            .params
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(",")
    };

    println!(
        "{} request module={} network={} production_mode={} partial_blocks={} param_modules={}",
        source_name, module, request_config.network, production_mode, partial_blocks, param_modules
    );

    let mut stream = endpoint
        .substreams(Request {
            start_block_num: start_block_offset,
            start_cursor: String::new(),
            stop_block_num: 0,
            final_blocks_only: false,
            package: Some(package),
            params: request_config.params,
            network: request_config.network,
            output_module: module,
            production_mode,
            debug_initial_store_snapshot_for_modules: vec![],
            dev_output_modules: vec![],
            limit_processed_blocks: u64::MAX,
            progress_messages_interval_ms: 1_000,
            partial_blocks,
            noop_mode: false,
        })
        .await?;

    let mut seen_blocks = 0usize;
    let mut current_block = None;
    let mut final_block = None;
    let mut progress_counter = 0u64;

    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    return Ok(());
                }
            }
            message = stream.next() => {
                let Some(response) = message else {
                    return Ok(());
                };
                let receive_time_ms = now_ms();
                let response = response?;

                let Some(message) = response.message else {
                    continue;
                };

                let maybe_event = match message {
                    SubstreamsMessage::Session(session) => {
                        println!(
                            "{} session resolved_start={} chain_head={} before_start={} after_start={} trace_id={}",
                            source_name,
                            session.resolved_start_block,
                            session.chain_head,
                            session.effective_blocks_to_process_before_start_block,
                            session.effective_blocks_to_process_after_start_block,
                            session.trace_id
                        );
                        None
                    }
                    SubstreamsMessage::Progress(progress) => {
                        progress_counter += 1;
                        if progress_counter == 1 || progress_counter % 20 == 0 {
                            let active_jobs = progress.running_jobs.len();
                            let bytes_read = progress
                                .processed_bytes
                                .as_ref()
                                .map(|bytes| bytes.total_bytes_read)
                                .unwrap_or_default();
                            let bytes_written = progress
                                .processed_bytes
                                .as_ref()
                                .map(|bytes| bytes.total_bytes_written)
                                .unwrap_or_default();
                            println!(
                                "{} progress processed_blocks={} active_jobs={} stages={} bytes_read={} bytes_written={}",
                                source_name,
                                progress.processed_blocks,
                                active_jobs,
                                progress.stages.len(),
                                bytes_read,
                                bytes_written
                            );
                        }
                        None
                    }
                    SubstreamsMessage::FatalError(error) => {
                        return Err(anyhow!(
                            "{} fatal substreams error module={} reason={}",
                            source_name,
                            error.module,
                            error.reason
                        ));
                    }
                    other => substreams_message_to_event(
                        &source_name,
                        other,
                        receive_time_ms,
                        use_block_timestamps,
                    )?,
                };

                let Some(event) = maybe_event else { continue; };

                if let Some(samples) = stop_after_samples {
                    if should_stop_after_block(
                        event.block_number,
                        samples,
                        &mut current_block,
                        &mut seen_blocks,
                        &mut final_block,
                    ) {
                        return Ok(());
                    }
                }

                if event_tx.send(event).is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn substreams_message_to_event(
    source_name: &str,
    message: SubstreamsMessage,
    receive_time_ms: i64,
    use_block_timestamps: bool,
) -> anyhow::Result<Option<ProbeEvent>> {
    let data = match message {
        SubstreamsMessage::BlockScopedData(data) => data,
        SubstreamsMessage::BlockUndoSignal(signal) => {
            let block_ref = signal.last_valid_block;
            return Ok(Some(ProbeEvent {
                source: source_name.to_string(),
                source_kind: SourceKind::Substreams,
                event_kind: "undo",
                block_number: block_ref
                    .as_ref()
                    .map_or(0, |block| block.number),
                block_hash: block_ref
                    .as_ref()
                    .map(|block| block.id.clone()),
                block_time_ms: None,
                receive_time_ms,
                lag_ms: None,
                is_partial: false,
                is_last_partial: false,
            }));
        }
        _ => return Ok(None),
    };

    block_scoped_data_to_event(source_name, data, receive_time_ms, use_block_timestamps).map(Some)
}

fn block_scoped_data_to_event(
    source_name: &str,
    data: BlockScopedData,
    receive_time_ms: i64,
    use_block_timestamps: bool,
) -> anyhow::Result<ProbeEvent> {
    let event_kind = classify_substreams_event(&data);
    let clock = data
        .clock
        .as_ref()
        .context("substreams BlockScopedData missing clock")?;
    let block_time_ms = clock
        .timestamp
        .as_ref()
        .map(timestamp_to_ms)
        .transpose()?;

    Ok(ProbeEvent {
        source: source_name.to_string(),
        source_kind: SourceKind::Substreams,
        event_kind,
        block_number: clock.number,
        block_hash: Some(clock.id.clone()),
        block_time_ms,
        receive_time_ms,
        lag_ms: use_block_timestamps
            .then_some(block_time_ms)
            .flatten()
            .map(|block_time_ms| receive_time_ms - block_time_ms),
        is_partial: data.is_partial,
        is_last_partial: data.is_last_partial.unwrap_or(false),
    })
}

async fn run_rpc_ws_source(
    rpc_ws_url: String,
    use_block_timestamps: bool,
    stop_after_samples: Option<usize>,
    mut stop_rx: watch::Receiver<bool>,
    event_tx: tokio_mpsc::UnboundedSender<ProbeEvent>,
) -> anyhow::Result<()> {
    let (mut ws_stream, _) = connect_async(&rpc_ws_url)
        .await
        .with_context(|| format!("connect to rpc websocket {rpc_ws_url}"))?;

    ws_stream
        .send(WsMessage::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            })
            .to_string(),
        ))
        .await
        .context("send eth_subscribe newHeads request")?;

    let mut seen_blocks = 0usize;
    let mut current_block = None;
    let mut final_block = None;

    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    return Ok(());
                }
            }
            message = ws_stream.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let receive_time_ms = now_ms();
                let message = message?;
                let Some(text) = websocket_text(message)? else {
                    continue;
                };

                let Some(event) =
                    rpc_message_to_event(&text, receive_time_ms, use_block_timestamps)?
                else {
                    continue;
                };

                if let Some(samples) = stop_after_samples {
                    if should_stop_after_block(
                        event.block_number,
                        samples,
                        &mut current_block,
                        &mut seen_blocks,
                        &mut final_block,
                    ) {
                        return Ok(());
                    }
                }

                if event_tx.send(event).is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn rpc_message_to_event(
    text: &str,
    receive_time_ms: i64,
    use_block_timestamps: bool,
) -> anyhow::Result<Option<ProbeEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("error").is_some() {
        return Err(anyhow!("rpc websocket returned error payload: {value}"));
    }

    let Some(result) = value
        .get("params")
        .and_then(|params| params.get("result"))
    else {
        return Ok(None);
    };

    let Some(block_number) = result
        .get("number")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
    else {
        return Ok(None);
    };

    let block_time_ms = result
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        .map(|seconds| (seconds as i64) * 1_000);

    Ok(Some(ProbeEvent {
        source: "rpc-ws".to_string(),
        source_kind: SourceKind::Rpc,
        event_kind: "new_head",
        block_number,
        block_hash: result
            .get("hash")
            .and_then(Value::as_str)
            .map(str::to_string),
        block_time_ms,
        receive_time_ms,
        lag_ms: use_block_timestamps
            .then_some(block_time_ms)
            .flatten()
            .map(|block_time_ms| receive_time_ms - block_time_ms),
        is_partial: false,
        is_last_partial: false,
    }))
}

fn should_stop_after_block(
    block_number: u64,
    samples: usize,
    current_block: &mut Option<u64>,
    seen_blocks: &mut usize,
    final_block: &mut Option<u64>,
) -> bool {
    if final_block.is_some_and(|target| block_number != target) {
        return true;
    }

    if current_block.as_ref() != Some(&block_number) {
        *current_block = Some(block_number);
        *seen_blocks += 1;

        if *seen_blocks == samples {
            *final_block = Some(block_number);
        }
    }

    false
}

fn websocket_text(message: WsMessage) -> anyhow::Result<Option<String>> {
    match message {
        WsMessage::Text(text) => Ok(Some(text)),
        WsMessage::Binary(bytes) => Ok(Some(String::from_utf8(bytes)?)),
        WsMessage::Ping(_) | WsMessage::Pong(_) => Ok(None),
        WsMessage::Close(frame) => {
            if let Some(frame) = frame {
                bail!("rpc websocket closed: code={} reason={}", frame.code, frame.reason);
            }
            bail!("rpc websocket closed");
        }
        _ => Ok(None),
    }
}

fn classify_substreams_event(data: &BlockScopedData) -> &'static str {
    if !data.is_partial {
        return "full";
    }

    let is_first = data.partial_index == Some(0);
    let is_last = data.is_last_partial.unwrap_or(false);

    match (is_first, is_last) {
        (true, true) => "partial_first_last",
        (true, false) => "partial_first",
        (false, true) => "partial_last",
        (false, false) => "partial",
    }
}

fn lag_metric_keys(event: &ProbeEvent) -> Vec<String> {
    match event.source_kind {
        SourceKind::Rpc => vec![event.source.clone()],
        SourceKind::Substreams => match event.event_kind {
            "full" => vec![format!("{}:full", event.source)],
            "partial_first" => vec![format!("{}:first_partial", event.source)],
            "partial_last" => vec![format!("{}:last_partial", event.source)],
            "partial_first_last" => vec![
                format!("{}:first_partial", event.source),
                format!("{}:last_partial", event.source),
            ],
            _ => Vec::new(),
        },
    }
}

fn spawn_csv_writer(
    path: &Path,
) -> anyhow::Result<(mpsc::Sender<CsvMessage>, thread::JoinHandle<anyhow::Result<()>>)> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create csv directory {}", parent.display()))?;
        }
    }

    let file = File::create(path).with_context(|| format!("create csv file {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(
        b"source,event_kind,block_number,block_hash,block_time_ms,receive_time_ms,lag_ms,is_partial,is_last_partial,rpc_receive_time_ms,delta_vs_rpc_ms,notes\n",
    )?;

    let (tx, rx) = mpsc::channel();

    let handle = thread::spawn(move || -> anyhow::Result<()> {
        let mut buffered = 0usize;

        loop {
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(CsvMessage::Line(line)) => {
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                    buffered += 1;

                    if buffered >= 128 {
                        writer.flush()?;
                        buffered = 0;
                    }
                }
                Ok(CsvMessage::Shutdown) => break,
                Err(RecvTimeoutError::Timeout) => {
                    writer.flush()?;
                    buffered = 0;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        writer.flush()?;
        Ok(())
    });

    Ok((tx, handle))
}

fn csv_line<const N: usize>(cells: [String; N]) -> String {
    cells
        .into_iter()
        .map(|cell| csv_escape(&cell))
        .collect::<Vec<_>>()
        .join(",")
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn percentile(sorted: &[i64], percentile: f64) -> i64 {
    let index = ((sorted.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    sorted[index]
}

fn timestamp_to_ms(timestamp: &prost_types::Timestamp) -> anyhow::Result<i64> {
    let seconds_ms = timestamp
        .seconds
        .checked_mul(1_000)
        .context("timestamp seconds overflow")?;
    let nanos_ms = (timestamp.nanos as i64) / 1_000_000;
    seconds_ms
        .checked_add(nanos_ms)
        .context("timestamp millis overflow")
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    let trimmed = value.trim_start_matches("0x");
    if trimmed.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(trimmed, 16).ok()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_millis() as i64
}

fn opt_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_default()
}

fn bool_cell(value: bool) -> String {
    if value { "true" } else { "false" }.to_string()
}

fn fmt_ms(value: i64) -> String {
    format!("{value}ms")
}

fn fmt_clock_ms(value: i64) -> String {
    Local
        .timestamp_millis_opt(value)
        .single()
        .map(|dt| dt.format("%H:%M:%S%.3f").to_string())
        .unwrap_or_else(|| value.to_string())
}
