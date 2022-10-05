use std::{env, process};

mod libs;

fn main() {
    let args: Vec<String> = env::args().collect();

    let path: String;
    match args.get(1) {
        Some(v) => {
            path = String::from(v);
        },
        None => {
            println!("specify file");
            process::exit(1);
        }
    }

    let mut instance = libs::Importer::new(path);
    if let Err(err) = instance.import() {
        println!("{:?}", err);
    }
}
