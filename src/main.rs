use std::{env, process};

mod libs;

fn main() {

    let args: Vec<String> = env::args().collect();

    let path: String;
    if let Some(p) = args.get(1) {
        path = p.to_string();
    } else {
        println!("specify file");
        process::exit(1);
    }

    let mut instance = libs::Importer::new(path);
    let _ = instance.import();
}
