use clap::Parser;
use console::style;
use std::env;
use std::ffi::OsString;
use zb_cli::{
    cli::{Cli, Commands},
    commands,
    init::ensure_init,
    utils::get_root_path,
};
use zb_io::create_installer;

#[tokio::main]
async fn main() {
    let args: Vec<OsString> = {
        let mut args: Vec<OsString> = env::args_os().collect();
        if let Some(first_arg) = args.get_mut(1) {
            match first_arg.to_str() {
                Some("i") => *first_arg = OsString::from("install"),
                Some("b") => *first_arg = OsString::from("bundle"),
                Some("u") => *first_arg = OsString::from("uninstall"),
                Some("m") => *first_arg = OsString::from("migrate"),
                Some("ls") => *first_arg = OsString::from("list"),
                Some("g") => *first_arg = OsString::from("gc"),
                Some("r") => *first_arg = OsString::from("reset"),
                Some("c") => *first_arg = OsString::from("completion"),
                Some("x") => *first_arg = OsString::from("run"),
                _ => {}
            }
        }
        args
    };

    let cli = Cli::parse_from(args);

    if let Err(e) = run(cli).await {
        eprintln!("{} {}", style("error:").red().bold(), e);
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), zb_core::Error> {
    if let Commands::Completion { shell } = cli.command {
        return commands::completion::execute(shell);
    }

    let root = get_root_path(cli.root);
    let prefix = cli.prefix.unwrap_or_else(|| root.join("prefix"));

    if let Commands::Init { no_modify_path } = cli.command {
        return commands::init::execute(&root, &prefix, no_modify_path);
    }

    if !matches!(cli.command, Commands::Reset { .. }) {
        ensure_init(&root, &prefix, cli.auto_init)?;
    }

    let mut installer = create_installer(&root, &prefix, cli.concurrency)?;

    match cli.command {
        Commands::Init { .. } => unreachable!(),
        Commands::Completion { .. } => unreachable!(),
        Commands::Install { formulas, no_link } => {
            commands::install::execute(&mut installer, formulas, no_link).await
        }
        Commands::Bundle { file, no_link } => {
            commands::bundle::execute(&mut installer, &file, no_link).await
        }
        Commands::Uninstall { formulas, all } => {
            commands::uninstall::execute(&mut installer, formulas, all)
        }
        Commands::Migrate { yes, force } => {
            commands::migrate::execute(&mut installer, yes, force).await
        }
        Commands::List => commands::list::execute(&mut installer),
        Commands::Info { formula } => commands::info::execute(&mut installer, formula),
        Commands::Gc => commands::gc::execute(&mut installer),
        Commands::Reset { yes } => commands::reset::execute(&root, &prefix, yes),
        Commands::Run { formula, args } => {
            commands::run::execute(&mut installer, formula, args).await
        }
    }
}
