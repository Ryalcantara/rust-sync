use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::time::Duration;

// Styled progress bar creation
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

// Styled progress bar for overall progress
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let progress_bar = ProgressBar::new(total);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {msg}\n{wide_bar:.cyan/blue} {pos}/{len} [{elapsed_precise}] [{per_sec}] {eta}")
            .unwrap()
            .progress_chars("#>-"),
    );
    progress_bar.set_message(message.to_string());
    progress_bar.enable_steady_tick(Duration::from_millis(100)); 
    progress_bar
}

// Function to print banner and initialize UI
pub fn init_ui() {
    // Clear screen and set up initial display
    print!("\x1B[2J\x1B[1;1H");

    // Banner
    println!(
        "{} {} {}",
        "🚀".green(),
        "Database Synchronization Tool".bold().blue(),
        "🚀".green()
    );
    println!("{}", "Connecting and preparing for sync...".yellow());
}