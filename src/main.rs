use std::fs::File;
use std::io::{prelude::*, BufReader};
use serde_json::{Value};

use mysql::*;
use mysql::prelude::*;

struct Show {
    title: String,
    show_poster: String,
    show_url: String,
}

fn main() {
    // Open and read the file lines
    let path = "./sample.json";
    let file = File::open(path).expect("Unable to read file");
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().map(|l| l.expect("Could not parse line")).collect();

    // Connect to your my-sql database
    let url = Opts::from_url("mysql://root:password@localhost:3306/db").unwrap();
    let pool = Pool::new(url).unwrap();
    let mut conn = pool.get_conn().unwrap();

    // Loop through the lines
    for line in lines {

        // Parse the line into a JSON object serde_json::Value
        let v: Value = serde_json::from_str(&line).expect("Unable to parse");

        // Since we want to save the streaming services of the said show, we need to loop an array inside the JSON file.
        let l = v["watchAvailability"][0]["directUrls"].as_array().unwrap().len();
        for n in 0..l {
            let streaming_url = v["watchAvailability"][0]["directUrls"][n].as_str().clone();
            match streaming_url {
                Some(url) => {
                    // Display the streaming url.  I have to do this to remove the Some(url) warning. 
                    // Unused variables in Rust emits warnings
                    println!("{:?}", url);

                    // Create a vector (array of object).  This provides you the ability to process multiple objects upon saving
                    let shows = vec![
                        Show { 
                            title: v["title"].as_str().as_deref().unwrap_or("Error").to_string(),
                            show_poster: v["posterPath"].as_str().as_deref().unwrap_or("Error").to_string(),
                            show_url: v["watchAvailability"][0]["directUrls"][n].as_str().as_deref().unwrap_or("Error").to_string(),
                        },
                    ];  
                    //Execute an insert query
                    conn.exec_batch(
                        r"INSERT INTO `shows` (`title`, `show_poster`, `show_url`)
                        VALUES (:title, :show_poster, :show_url)",
                    shows.iter().map(|s| params! {
                        "title" => s.title.clone(),
                        "show_poster" => s.show_poster.clone(),
                        "show_url" => s.show_url.clone(),
                        })
                    ).unwrap_err();
                },
                _ => println!("Error"),
            }
        }

    }
}

