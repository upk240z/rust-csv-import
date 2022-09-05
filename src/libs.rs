use std::fs::File;
use std::io::{BufReader, BufRead};
use std::{io, process, str, env};
use encoding_rs;
use kanaria::UCSStr;
use chrono::{Datelike, Local};
use dotenv::dotenv;
use sqlx::Executor;
use sqlx::mysql::{MySqlPoolOptions, MySqlPool};

pub struct Importer {
    path: String,
    pool: MySqlPool,
    rows: i32,
    now_ym: i32,
}

impl Importer {
    pub async fn new(path: String) -> Importer {
        dotenv().ok();
        let uri = env::var("MYSQL_URI").expect("MYSQL_URI not found");
        let result = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(uri.as_str()).await;

        match result {
            Ok(pool) => {
                Importer {
                    path,
                    pool,
                    rows: 0,
                    now_ym: 0,
                }
            },
            Err(err) => {
                println!("{:?}", err);
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

    pub async fn import(&mut self) -> io::Result<()> {
        let now = Local::now().date();
        self.now_ym = format!("{:>04}{:>02}", now.year(), now.month()).parse().unwrap();
        let table_name = env::var("TABLE_NAME").expect("TABLE_NAME not found");

        let truncate_result = self.pool.execute(sqlx::query(
            format!("TRUNCATE {}", table_name).as_str()
        )).await;
        if truncate_result.is_err() {
            println!("TRUNCATE error");
            process::exit(1);
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
            match columns[40] {
                "000000" => { columns[40] = "999999"; },
                _ => {}
            }

            let percent = counter as f32 / self.rows as f32 * 100.0;
            println!("{} ({}/{}) {:>6.02}%", columns[0], counter, self.rows, percent);

            let stop_ym: i32 = columns.get(40).unwrap().parse().unwrap();
            if stop_ym < self.now_ym {
                continue;
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
                    ?,
                    NULLIF(?, ''),
                    NULLIF(?, ''),
                    NULLIF(?, ''),
                    NULLIF(?, ''),
                    NULLIF(?, ''),
                    NULLIF(?, ''),
                    ?,
                    ?
                )
            ", table_name);

            let query = sqlx::query(sql.as_str())
                .bind(columns[0])
                .bind(columns[2])
                .bind(columns[20])
                .bind(columns[21])
                .bind(columns[22])
                .bind(UCSStr::from_str(columns[11]).wide().to_string())
                .bind(UCSStr::from_str(columns[12]).wide().to_string())
                .bind(columns[39])
                .bind(columns[40]);

            if let Err(err) = self.pool.execute(query).await {
                println!("{:?}", err);
                process::exit(1);
            }
        }

        Ok(())
    }
}
