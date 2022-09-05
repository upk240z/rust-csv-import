use std::fs::File;
use std::io::{BufReader, BufRead};
use std::{env, io, process, str};
use encoding_rs;
use mysql::{params, Pool, PooledConn, Statement};
use kanaria::UCSStr;
use chrono::{Datelike, Local};
use dotenv::dotenv;
use mysql::prelude::Queryable;

pub struct Importer {
    path: String,
    conn: PooledConn,
    rows: i32,
    now_ym: i32,
    stmt: Option<Statement>,
}

impl Importer {
    pub fn new(path: String) -> Importer {
        dotenv().ok();
        let uri = env::var("MYSQL_URI").expect("MYSQL_URI not found");
        let pool_result = Pool::new(uri.as_str());
        match pool_result {
            Ok(pool) => {
                match pool.get_conn() {
                    Ok(conn) => {
                        Importer {
                            path,
                            conn,
                            rows: 0,
                            now_ym: 0,
                            stmt: None,
                        }
                    },
                    Err(err) => {
                        println!("{}", err);
                        process::exit(1);
                    }
                }
            },
            Err(err) => {
                println!("{}", err);
                process::exit(1);
            }
        }
    }

    fn count_rows(&mut self) -> io::Result<()> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        self.rows = 0;
        reader.lines().for_each(|_| self.rows += 1);
        Ok(())
    }

    pub fn import(&mut self) -> io::Result<()> {
        let now = Local::now().date();
        self.now_ym = format!("{:>04}{:>02}", now.year(), now.month()).parse().unwrap();
        let table_name = env::var("TABLE_NAME").expect("TABLE_NAME not found");

        let truncate_result = self.conn.exec_drop(format!("TRUNCATE {}", table_name), ());
        if truncate_result.is_err() {
            println!("TRUNCATE error");
            process::exit(1);
        }

        let sql = format!("
            INSERT INTO {} (
                code,
                zipcode,
                city,
                town,
                chome,
                city_kana,
                town_kana,
                start_ym,
                end_ym
            ) VALUES (
                :code,
                NULLIF(:zipcode, ''),
                NULLIF(:city, ''),
                NULLIF(:town, ''),
                NULLIF(:chome, ''),
                NULLIF(:city_kana, ''),
                NULLIF(:town_kana, ''),
                :start_ym,
                :end_ym
            )
        ", table_name);

        match self.conn.prep(sql) {
            Ok(stmt) => {
                self.stmt = Some(stmt);
            },
            Err(err) => {
                println!("{}", err);
                process::exit(1);
            }
        }

        if self.count_rows().is_err() {
            println!("row count error");
            process::exit(1);
        }

        println!("total rows: {}", self.rows);

        let file = File::open(&self.path)?;
        let mut counter = 0;
        let reader = BufReader::new(file);
        let mut stream = reader.split(0x0a);

        while let Some(r) = stream.next() {
            counter += 1;
            let bytes = r.unwrap();

            let (res, _, _) = encoding_rs::SHIFT_JIS.decode(&bytes);
            let converted = String::from(res.into_owned()).replace(" ", "").replace("　", "");
            let mut columns: Vec<&str> = converted.trim().split(",").collect();

            match columns.get(40).unwrap().as_ref() {
                "000000" => { columns[40] = "999999"; },
                _ => {}
            }

            let stop_ym: i32 = columns.get(40).unwrap().parse().unwrap();
            if stop_ym < self.now_ym {
                continue;
            }

            if let Some(stmt) = &self.stmt {
                let parameters = params! {
                    "code" => columns[0],
                    "zipcode" => columns[2],
                    "city" => columns[20],
                    "town" => columns[21],
                    "chome" => columns[22],
                    "city_kana" => UCSStr::from_str(columns[11]).wide().to_string(),
                    "town_kana" => UCSStr::from_str(columns[12]).wide().to_string(),
                    "start_ym" => columns[39],
                    "end_ym" => columns[40],
                };

                if let Err(err) = self.conn.exec_drop(stmt, parameters) {
                    println!("{:?}", err);
                }
            }

            let percent = counter as f32 / self.rows as f32 * 100.0;
            println!("{} ({}/{}) {:>6.02}%", columns[0], counter, self.rows, percent);
        }

        Ok(())
    }
}
