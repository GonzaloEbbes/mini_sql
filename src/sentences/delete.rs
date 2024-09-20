use crate::errors::apperrors::MiniSQLError;
use super::common::{get_headers, add_all_fields, validate_table, format_to_csv};
use super::conditions::get_query;
use crate::file;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader};


struct Delete {
    target_table: String,
    condition: Vec<String>,
}

fn new_delete(sentence_parts: Vec<String>) -> Result<Delete, MiniSQLError> {
    let (condition, table) = decode_delete(sentence_parts)?;
    Ok(Delete {
        target_table: table,
        condition,
    })
}

pub fn execute_delete_statement(
    sententence_vec: Vec<String>,
    route: &String,
) -> Result<(), MiniSQLError> {
    let delete = new_delete(sententence_vec)?;
    let file_iter = file::handler::new_file_iterator(route, &delete.target_table)?;

    execute_delete(&delete, file_iter, route)?;
    Ok(())
}

fn decode_delete(sentence_parts: Vec<String>) -> Result<(Vec<String>, String), MiniSQLError> {
    let mut condition: Vec<String> = Vec::new();
    let mut from: Vec<String> = Vec::new();

    (condition, from) = match_fields_delete(sentence_parts, condition, from)?;

    let table = validate_table(from)?;

    Ok((condition, table))
}

fn match_fields_delete(
    sentence_parts: Vec<String>,
    mut condition: Vec<String>,
    mut from: Vec<String>,
) -> Result<(Vec<String>, Vec<String>), MiniSQLError> {
    let mut base = "";
    for part in &sentence_parts {
        match part.as_str() {
            "DELETE" => {
                base = "";
                continue;
            }
            "FROM" => {
                base = "from";
                continue;
            }
            "WHERE" => {
                base = "condition";
                continue;
            }
            _ => (),
        }

        match base {
            "condition" => condition.push(part.to_string()),
            "from" => from.push(part.to_string()),
            _ => {
                return Err(MiniSQLError::InvalidSyntax(format!(
                    "Invalid sentence: {} ",
                    sentence_parts.join(" ")
                )))
            }
        }
    }
    Ok((condition, from))
}

fn execute_delete(
    sentence: &Delete,
    file_iter: BufReader<File>,
    file_path: &String,
) -> Result<(), MiniSQLError> {
    let (file_iter, headers) = get_headers(file_iter);
    let mapped_fields = add_all_fields(&headers);
    let mut new_file = file::handler::create_file(file_path, &sentence.target_table)?;

    let headers = headers.join(",").replace("\n", "");
    writeln!(new_file, "{}", headers)?;
    for result in file_iter.lines() {
        let record = result?;
        let line = format_to_csv(record);
        let should_apply = get_query(
            &sentence.condition,
            0,
            sentence.condition.len(),
            &mapped_fields,
            &line,
        )?;
        if !should_apply {
            let csv_line = line.join(",").replace("\n", "");
            writeln!(new_file, "{}", csv_line)?;
        }
    }
    file::handler::rename_file(file_path, &sentence.target_table)?;

    Ok(())
}

