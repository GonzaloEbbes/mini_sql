use std::env;
mod errors;
use crate::errors::apperrors::MiniSQLError;
pub mod file;
pub mod sentences;
fn main() {
    let args: Vec<String> = env::args().collect();

    match get_args(args) {
        Ok((route, sentence)) => match execute_query(route.to_string(), sentence.to_string()) {
            Ok(()) => (),
            Err(error) => eprintln!("{}", error),
        },
        Err(error) => eprintln!("{}", error),
    }
}

fn get_args(mut args: Vec<String>) -> Result<(String, String), MiniSQLError> {
    let arg2 = args.pop().ok_or_else(|| {
        MiniSQLError::InvalidSyntax("Missing second parameter: SQL query.".to_string())
    })?;

    let arg1 = args.pop().ok_or_else(|| {
        MiniSQLError::InvalidSyntax("Missing first parameter: path to dir.".to_string())
    })?;

    Ok((arg1, arg2))
}

fn execute_query(route: String, sentence: String) -> Result<(), MiniSQLError> {
    let sententence_vec: Vec<String> = standardize_sentence(sentence);

    if let Some(sentence_type) = sententence_vec.first() {
        match sentence_type.to_uppercase().as_str() {
            "SELECT" => sentences::select::execute_select_statement(sententence_vec, &route),
            "DELETE" => sentences::delete::execute_delete_statement(sententence_vec, &route),
            "INSERT" => sentences::insert::execute_insert_statement(sententence_vec, &route),
            "UPDATE" => sentences::update::execute_update_statement(sententence_vec, &route),
            _ => Err(MiniSQLError::InvalidSyntax(format!(
                "Unable recognize statement: {}",
                sentence_type.to_uppercase().as_str()
            ))),
        }
    } else {
        Err(MiniSQLError::InvalidTable("Empty statement".to_string()))
    }
}

fn standardize_sentence(sentence: String) -> Vec<String> {
    let sentence_vec: Vec<String> = sentence.split("'").map(|s| s.to_string()).collect();
    let mut result: Vec<String> = Vec::new();

    for (i, part) in sentence_vec.iter().enumerate() {
        if i % 2 == 0 {
            let mut modified_part = String::new();

            for c in part.chars() {
                match c {
                    '(' | ')' | ',' => {
                        modified_part.push(' ');
                        modified_part.push(c);
                        modified_part.push(' ');
                    }
                    ';' | '\n' | '\t' => modified_part.push(' '),
                    _ => modified_part.push(c),
                }
            }

            let replaced: Vec<String> = modified_part
                .split_ascii_whitespace()
                .map(|s| s.to_string())
                .collect();

            result.extend(replaced);
        } else {
            result.push(part.to_string());
        }
    }

    result
}
