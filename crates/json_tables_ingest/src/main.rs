use clap::Parser;
use json_to_parquet::{run, Args};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    run(args)
}
