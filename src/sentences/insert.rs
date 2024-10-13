use super::common::{add_all_fields, get_headers, get_required_fields, validate_table};
use crate::errors::apperrors::MiniSQLError;
use crate::file;
use std::collections::HashMap;
use std::io::Write;

/// Executes a `INSERT` query with the provided SQL string.
///
/// This function encapsulates the entire lifecycle of a `INSERT`,
/// including the creation, execution, and handling of the query.
///
/// Will append the registers to the table
///
/// # Examples
///
/// ```
/// execute_insert_statement(["INSERT", "INTO", "clientes", "(", "nombre", ",", "apellido", ")", "VALUES", "(", "'pepe'", ",", "'garcia'", ")"], &"user/data/tables");
/// execute_insert_statement(["INSERT", "INTO", "clientes", "(", "id_cliente", ",", "nombre", ",", "apellido", ",", "email", ",", "telefono", ")", "VALUES", "(", "111", ",", "'pepe'", ",", "'garcia'", ",", "'pepe@email.com'", ",", "5551234990", ")"], &"user/data/tables");
/// execute_insert_statement(["INSERT", "INTO", "clientes", "(", "nombre", ",", "apellido", ")", "VALUES", "(", "'pepe'", ",", "'garcia'", ")",",", "(", "carlos", ",", "rodriguez", ")"], &"user/data/tables");
/// ```
///
/// # Errors
///
/// This function will return an error of type `MiniSQLError` if:
///
/// - The SQL string is invalid.
/// - The provided table is invalid.
/// - The query fails for any other reason.
///
/// # Returns
///
/// - `Ok(())` if the query executes successfully.
/// - `Err(MiniSQLError)` if an error occurs during execution.
///  
pub fn execute_insert_statement(
    sententence_vec: Vec<String>,
    route: &String,
) -> Result<(), MiniSQLError> {
    let insert = new_insert(sententence_vec)?;
    execute_insert(&insert, route)?;
    Ok(())
}

/// Contains all requiered data to execute a INSERT statement given row values
struct Insert {
    /// INTO --> target_table
    target_table: String,
    /// ( nombre , apellido ) --> fields; ["nombre", "apellido"]
    fields: Vec<String>,
    /// VALUES ('pepe', 'garcia'), ('carlos', 'rodriguez') --> values ; as vector containing each new register as a vector of strings
    values: Vec<Vec<String>>,
}

struct FieldsToInsert {
    fields: Vec<String>,
    target_table: Vec<String>,
    values: Vec<Vec<String>>,
}

fn new_insert(sentence_parts: Vec<String>) -> Result<Insert, MiniSQLError> {
    decode_insert(sentence_parts)
}

fn decode_insert(sentence_parts: Vec<String>) -> Result<Insert, MiniSQLError> {
    let fields: Vec<String> = Vec::new();
    let from: Vec<String> = Vec::new();
    let values: Vec<Vec<String>> = Vec::new();

    let fields_to_insert = match_fields_insert(sentence_parts, fields, from, values)?;

    let table = validate_table(fields_to_insert.target_table)?;

    Ok(Insert {
        target_table: table,
        fields: fields_to_insert.fields,
        values: fields_to_insert.values,
    })
}

fn match_fields_insert(
    sentence_parts: Vec<String>,
    mut fields: Vec<String>,
    mut from: Vec<String>,
    mut values: Vec<Vec<String>>,
) -> Result<FieldsToInsert, MiniSQLError> {
    let mut value: Vec<String> = Vec::new();

    let mut base = "";
    for part in &sentence_parts {
        match part.as_str() {
            "INSERT" => {
                base = "from";
                continue;
            }
            "INTO" => {
                if base == "from" {
                    base = "from table"
                }
                continue;
            }
            "(" => {
                if base == "from table" {
                    base = "fields ("
                } else if base == "values" {
                    base = "values ("
                }
                continue;
            }
            ")" => {
                if base == "fields (" {
                    base = "fields ()"
                } else if base == "values (" {
                    base = "values ()";
                    values.push(value.clone());
                    value = Vec::new();
                }
                continue;
            }
            "," => {
                if base == "values ()" {
                    base = "values (";
                }
                continue;
            }
            "VALUES" => {
                if base != "fields ()" {
                    return Err(MiniSQLError::InvalidSyntax(format!(
                        "Invalid sentence VALUES were given but missing fields to be replaced: {} ",
                        sentence_parts.join(" ")
                    )));
                } else {
                    base = "values"
                }
                continue;
            }
            _ => (),
        }
        match base {
            "from table" => from.push(part.to_string()),
            "fields (" => fields.push(part.to_string()),
            "values (" => value.push(part.to_string()),
            _ => {
                return Err(MiniSQLError::InvalidSyntax(format!(
                    "Invalid sentence: {} ",
                    sentence_parts.join(" ")
                )))
            }
        }
    }

    for value in &values {
        if value.len() != fields.len() {
            let message = format!(
                "Invalid sentence VALUE, too little arguments were sent: {} 
            \n. {} were requiered but sent {} ",
                value.join(" "),
                fields.len(),
                values.len()
            );
            return Err(MiniSQLError::InvalidSyntax(message));
        }
    }
    Ok(FieldsToInsert {
        fields,
        target_table: from,
        values,
    })
}

fn execute_insert(sentence: &Insert, route: &String) -> Result<(), MiniSQLError> {
    let headers: Vec<String>;
    {
        let file = file::handler::new_file_iterator(route, &sentence.target_table)?;
        let (_, headers_file) = get_headers(file);
        headers = headers_file
    };

    let mapped_fields = add_all_fields(&headers);
    let mut new_file = file::handler::create_file_append(route, &sentence.target_table)?;
    let indexes = get_required_fields(&sentence.fields, &headers)?;

    for line in &sentence.values {
        let formatted_line =
            format_new_line(line, &indexes, &sentence.fields, mapped_fields.len())?;
        let csv_line = formatted_line.join(",").replace("\n", "");
        writeln!(new_file, "{}", csv_line)?;
    }

    Ok(())
}

fn format_new_line(
    line: &[String],
    indexes: &HashMap<String, usize>,
    requiered_fields: &[String],
    line_size: usize,
) -> Result<Vec<String>, MiniSQLError> {
    let mut base_line = vec!["".to_string(); line_size];

    for (index, field) in requiered_fields.iter().enumerate() {
        if let Some(line_index) = indexes.get(field) {
            if let Some(inserted_value) = line.get(index) {
                base_line[*line_index] = inserted_value.to_string();
            } else {
                // no deberia ocurrir pero cortamos el flujo si ocurre
                return Err(MiniSQLError::Generic(format!(
                    "program found unexpected error while inserting new lines: {} ",
                    requiered_fields.join(" ")
                )));
            }
        } else {
            // no deberia ocurrir pero cortamos el flujo si ocurre
            return Err(MiniSQLError::Generic(format!(
                "program found unexpected error while inserting new lines: {} ",
                requiered_fields.join(" ")
            )));
        }
    }
    Ok(base_line)
}
