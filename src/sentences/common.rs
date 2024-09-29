use crate::errors::apperrors::MiniSQLError;
use std::fs::File;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};



pub fn get_headers(mut file_iter: BufReader<File>) -> (BufReader<File>, Vec<String>) {
    let mut buffer: String = String::new();
    let _ = file_iter.read_line(&mut buffer);
    (file_iter, format_to_csv(buffer.replace("\n", "")))
}

pub fn format_to_csv(buffer: String) -> Vec<String> {
    let vec: Vec<String> = buffer.split(",").map(|s| s.to_string()).collect();
    vec
}

pub fn get_required_fields(
    query_fields: &[String],
    headers: &[String],
) -> Result<HashMap<String, usize>, MiniSQLError> {
    if query_fields.len() == 1 && query_fields[0] == "*" {
        let all_fields: HashMap<String, usize> = add_all_fields(headers);
        return Ok(all_fields);
    }

    let mut indexes: HashMap<String, usize> = HashMap::new();
    for field in query_fields {
        if field == "(" || field == ")" || field == "," {
            continue;
        }
        let mut index = 0;
        let mut found = false;
        for csv_header in headers {
            if field == csv_header {
                indexes.insert(field.clone(), index);
                found = true;
                continue;
            }
            index += 1;
        }
        if !found {
            return Err(MiniSQLError::InvalidColumn(format!(
                "requested field [ {} ] could not be found",
                field
            )));
        }
    }

    Ok(indexes)
}

pub fn add_all_fields(headers: &[String]) -> HashMap<String, usize> {
    let mut indexes: HashMap<String, usize> = HashMap::new();
    for (index, row) in headers.iter().enumerate() {
        if index == headers.len()-1{
            indexes.insert(row.to_string().replace("\n", ""), index);
        }else{
            indexes.insert(row.to_string(), index);
        }
    }

    indexes
}

pub fn validate_table(mut from: Vec<String>) -> Result<String, MiniSQLError> {
    let table = if let Some(first) = from.pop() {
        first
    } else {
        return Err(MiniSQLError::InvalidTable(
            "no table was given ".to_string(),
        ));
    };

    if !from.is_empty() {
        from.push(table);
        return Err(MiniSQLError::InvalidTable(format!(
            "multiple references for table: {} ",
            from.join(" ")
        )));
    }
    Ok(table)
}