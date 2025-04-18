use colored::*;
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use tokio::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Styled progress bar creation with enhanced visualization
pub fn create_spinner(message: &str) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.blue} {msg} [{elapsed_precise}]")
            .unwrap(),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(Duration::from_millis(100)); 
    spinner
}

// Styled progress bar for overall progress with enhanced visuals
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let progress_bar = ProgressBar::new(total);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {msg}\n{wide_bar:.cyan/blue} {pos}/{len} ({percent}%) [{elapsed_precise}] [{per_sec}] {eta}")
            .unwrap()
            .progress_chars("█▓▒░"),
    );
    progress_bar.set_message(message.to_string());
    progress_bar.enable_steady_tick(Duration::from_millis(100)); 
    progress_bar
}

// Create a multi-progress display for showing multiple progress bars
pub fn create_multi_progress() -> MultiProgress {
    MultiProgress::new()
}

// Create a counter for real-time statistics
pub fn create_counter() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(0))
}

// Create a transfer progress display with speed information
pub fn create_transfer_progress(total: u64, label: &str) -> ProgressBar {
    let progress = ProgressBar::new(total);
    progress.set_style(
        ProgressStyle::default_bar()
            .template(&format!("{{spinner:.green}} {} {{bar:40.cyan/blue}} {{pos}}/{{len}} ({{percent}}%) [{{elapsed_precise}}] [{{binary_bytes_per_sec}}] {{eta}}", label))
            .unwrap()
            .progress_chars("█▇▆▅▄▃▂▁"),
    );
    progress.enable_steady_tick(Duration::from_millis(100));
    progress
}

// Function to print banner and initialize UI with version information
pub fn init_ui() {
    // Clear screen and set up initial display
    print!("\x1B[2J\x1B[1;1H");

    // Banner with enhanced styling
    println!("{}", "╔═══════════════════════════════════════════╗".bright_blue());
    println!("{} {} {}",
        "║".bright_blue(),
        " 🚀 Database Synchronization Tool v0.1.0 🚀 ".bold().white().on_blue(),
        "║".bright_blue()
    );
    println!("{}", "╚═══════════════════════════════════════════╝".bright_blue());
    
    println!("{}", "✨ Initializing and preparing for sync...".yellow().bold());
    println!("{}", "───────────────────────────────────────────".bright_black());
}