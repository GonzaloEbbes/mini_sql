use crate::errors::apperrors::MiniSQLError;
use std::fs;
use std::fs::File;
use std::io::BufReader;

pub fn new_file_iterator(
    dir: &String,
    file_name: &String,
) -> Result<BufReader<File>, MiniSQLError> {
    let route: String = format!("{}/{}{}", dir, file_name, ".csv");
    let file = File::open(route);
    match file {
        Ok(file) => {
            let reader = BufReader::new(file);
            Ok(reader)
        }
        Err(_) => Err(MiniSQLError::InvalidTable(format!(
            "Unable to open file at {}/{}",
            dir, file_name
        ))),
    }
}

pub fn create_file(route: &String, name: &String) -> Result<File, MiniSQLError> {
    let route: String = format!("{}/{}{}", route, name, ".temp");
    let file = File::create(route);
    match file {
        Ok(file) => Ok(file),
        Err(err) => Err(MiniSQLError::Generic(format!(
            "there was a problem updating the table: {} ",
            err
        ))),
    }
}

pub fn rename_file(route: &String, name: &String) -> Result<(), MiniSQLError> {
    let previous_path: String = format!("{}/{}{}", route, name, ".temp");
    let new_path: String = format!("{}/{}{}", route, name, ".csv");
    let rename = fs::rename(previous_path, new_path);
    match rename {
        Ok(file) => Ok(file),
        Err(err) => Err(MiniSQLError::Generic(format!(
            "there was a problem appliying changes to the table: {} ",
            err
        ))),
    }
}

pub fn create_file_append(route: &String, name: &String) -> Result<File, MiniSQLError> {
    let route: String = format!("{}/{}{}", route, name, ".csv");
    let file = File::options().append(true).open(route);
    match file {
        Ok(file) => Ok(file),
        Err(err) => Err(MiniSQLError::Generic(format!(
            "there was a problem inserting into the table: {} ",
            err
        ))),
    }
}
