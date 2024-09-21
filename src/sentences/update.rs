use crate::errors::apperrors::MiniSQLError;
use super::common::{get_headers, add_all_fields, validate_table, format_to_csv};
use super::conditions::get_query;
use crate::file;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::collections::HashMap;

/// Ejecuta una consulta `UPDATE` con la cadena SQL proporcionada.
///
/// Esta función encapsula todo el ciclo de vida de un `UPDATE`,
/// incluyendo la creación, ejecución y manejo de la consulta.
/// 
/// # Ejemplos
///
/// ```
/// execute_update_statement(["UPDATE", "clientes", "SET", "email", "=", "'pepe@hotmail.com'", ",", "nombre", "=", "'pepe'", "WHERE", "id", ">", "108"], &"user/data/tables");
/// ```
///
/// # Errores
///
/// Esta función retornará un error de tipo `MiniSQLError` si:
///
/// - La cadena SQL es inválida.
/// - La tabla proporcionada es invalida.
/// - La consulta falla por algún otro motivo.
///
/// # Retornos
///
/// - `Ok(())` si la consulta se ejecuta exitosamente.
/// - `Err(MiniSQLError)` si ocurre un error durante la ejecución.
/// 
pub fn execute_update_statement(
    sententence_vec: Vec<String>,
    route: &String,
) -> Result<(), MiniSQLError> {
    let update = new_update(sententence_vec)?;
    let file_iter = file::handler::new_file_iterator(route, &update.target_table)?;

    execute_update(&update, file_iter, route)?;
    Ok(())
}

struct Update {
    target_table: String,
    fields: Vec<(String, String)>,
    condition: Vec<String>,
}
struct FieldsToUpdate {
    target_table: Vec<String>,
    fields: Vec<String>,
    condition: Vec<String>,
}

fn new_update(sentence_parts: Vec<String>) -> Result<Update, MiniSQLError> {
    decode_update(sentence_parts)
}

fn decode_update(sentence_parts: Vec<String>) -> Result<Update, MiniSQLError> {
    let fields_raw: Vec<String> = Vec::new();
    let condition: Vec<String> = Vec::new();
    let from: Vec<String> = Vec::new();

    let fields_to_update = match_fields_update(sentence_parts, fields_raw, condition, from)?;

    let table = validate_table(fields_to_update.target_table)?;

    let formatted_fields = format_fields_to_update(fields_to_update.fields)?;

    Ok(Update {
        target_table: table,
        fields: formatted_fields,
        condition: fields_to_update.condition,
    })
}

fn match_fields_update(
    sentence_parts: Vec<String>,
    mut fields_raw: Vec<String>,
    mut condition: Vec<String>,
    mut from: Vec<String>,
) -> Result<FieldsToUpdate, MiniSQLError> {
    let mut base = "";
    for part in &sentence_parts {
        match part.as_str() {
            "UPDATE" => {
                base = "from";
                continue;
            }
            "SET" => {
                base = "fields";
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
            "fields" => fields_raw.push(part.to_string()),
            _ => {
                return Err(MiniSQLError::InvalidSyntax(format!(
                    "Invalid sentence: {} ",
                    sentence_parts.join(" ")
                )))
            }
        }
    }
    Ok(FieldsToUpdate {
        target_table: from,
        fields: fields_raw,
        condition,
    })
}

fn format_fields_to_update(raw_fields: Vec<String>) -> Result<Vec<(String, String)>, MiniSQLError> {
    let mut formatted_fields: Vec<(String, String)> = vec![];
    let mut index: usize = 0;
    let mut str1: String = String::from("");

    for field in raw_fields {
        match index {
            0 => str1 = field,
            1 => {
                if field != "=" {
                    return Err(MiniSQLError::InvalidSyntax(format!(
                        "Invalid syntax for update, should be a asignation symbol: {} ",
                        field
                    )));
                }
            }
            2 => formatted_fields.push((str1.to_string(), field)),
            3 => {
                if field == "," {
                    index = 0;
                    continue;
                }
                return Err(MiniSQLError::InvalidSyntax(format!(
                    "Invalid syntax for update, sentence should follow KEY = VALUE format, but has a fourth part: {} ", field)));
            }
            _ => {
                return Err(MiniSQLError::Generic(
                    "program found unexpected error while parsing update".to_string(),
                ))
            }
        }
        index += 1;
    }

    Ok(formatted_fields)
}

fn execute_update(
    sentence: &Update,
    file_iter: BufReader<File>,
    file_path: &String,
) -> Result<(), MiniSQLError> {
    let (file_iter, headers) = get_headers(file_iter);
    let mapped_fields = add_all_fields(&headers);
    let (indexes_to_modify, values) = get_fields_to_update(&sentence.fields, &mapped_fields)?;

    let mut new_file = file::handler::create_file(file_path, &sentence.target_table)?;

    let headers = headers.join(",").replace("\n", "");
    writeln!(new_file, "{}", headers)?;
    for result in file_iter.lines() {
        let record = result?;
        let mut line = format_to_csv(record);
        let should_apply = get_query(
            &sentence.condition,
            0,
            sentence.condition.len(),
            &mapped_fields,
            &line,
        )?;
        if should_apply {
            line = update_line(line, &indexes_to_modify, &values)?;
        }
        let csv_line = line.join(",").replace("\n", "");
        writeln!(new_file, "{}", csv_line)?;
    }
    file::handler::rename_file(file_path, &sentence.target_table)?;
    Ok(())
}

fn get_fields_to_update(
    fields: &[(String, String)],
    mapped_fields: &HashMap<String, usize>,
) -> Result<(Vec<usize>, Vec<String>), MiniSQLError> {
    let mut indexes: Vec<usize> = vec![];
    let mut values: Vec<String> = vec![];

    for field in fields {
        if let Some(index) = mapped_fields.get(&field.0) {
            indexes.push(*index);
            values.push(field.1.clone())
        } else {
            return Err(MiniSQLError::InvalidSyntax(format!(
                "Invalid sentence field {} was not found in table.",
                field.0
            )));
        }
    }

    Ok((indexes, values))
}

fn update_line(
    line: Vec<String>,
    indexes_to_modify: &[usize],
    values: &[String],
) -> Result<Vec<String>, MiniSQLError> {
    let mut new_line: Vec<String> = vec![];

    for (index, column) in line.iter().enumerate() {
        let mut found = false;
        for (inner_index, value) in indexes_to_modify.iter().enumerate() {
            if index == *value {
                if let Some(new_value) = values.get(inner_index) {
                    new_line.push(new_value.to_string());
                    found = true
                } else {
                    return Err(MiniSQLError::Generic(format!(
                        "program found unexpected error while updating a line: {} \n
                     new fields: {} ",
                        line.join(" "),
                        values.join(" ")
                    )));
                }
            }
        }
        if !found {
            new_line.push(column.to_string());
        }
    }

    Ok(new_line)
}
