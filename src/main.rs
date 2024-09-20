use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::vec;
mod errors;
use sentences::common::{get_headers, add_all_fields, format_to_csv, get_required_fields};
use crate::errors::apperrors::MiniSQLError;
pub mod sentences;
pub mod file;
fn main() {
    let args: Vec<String> = env::args().collect();

    match get_args(&args) {
        Ok((route, sentence)) => match execute_query(route.to_string(), sentence.to_string()) {
            Ok(()) => (),
            Err(error) => eprintln!("{}", error),
        },
        Err(error) => eprintln!("{}", error),
    }
}

fn get_args(args: &[String]) -> Result<(&String, &String), MiniSQLError> {
    let arg1 = &args.get(1).ok_or_else(|| {
        MiniSQLError::InvalidSyntax("Missing first parameter: path to dir.".to_string())
    })?;
    let arg2 = &args.get(2).ok_or_else(|| {
        MiniSQLError::InvalidSyntax("Missing second parameter: SQL query.".to_string())
    })?;
    Ok((arg1, arg2))
}

fn execute_query(route: String, sentence: String) -> Result<(), MiniSQLError> {
    let sententence_vec: Vec<String> = standardize_sentence(sentence);

    if let Some(sentence_type) = sententence_vec.first() {
        match sentence_type.to_uppercase().as_str() {
            "SELECT" => execute_select_statement(sententence_vec, &route),
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
    let sentence_vec: Vec<String> = sentence
        .replace("\n", " ")
        .replace(",", " , ")
        .split("'")
        .map(|s| s.to_string())
        .collect();
    let mut result: Vec<String> = Vec::new();

    for (i, part) in sentence_vec.iter().enumerate() {
        if i % 2 == 0 {
            let replaced: Vec<String> = part
                .replace("(", " ( ")
                .replace(")", " ) ")
                .replace(";", " ")
                .replace("\t", " ")
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





/// Contains all requiered data to execute a SELECT statement give row values
struct Select {
    /// FROM --> target_table
    target_table: String,
    /// WHERE --> condition ; as a vector of each part, id = 1 --> ["id", "=", "1"]
    condition: Vec<String>,
    /// SELECT --> mapped_fields; a vector contained fields requiered to display
    mapped_fields: Vec<String>,
    /// ORDER BY --> order ; if type is absent ASC will be taken as default
    order: Vec<String>,
}

struct FieldsToSelect {
    target_table: Vec<String>,
    condition: Vec<String>,
    mapped_fields: Vec<String>,
    order: Vec<String>,
}

fn new_select(sentence_parts: Vec<String>) -> Result<Select, MiniSQLError> {
    decode_select(sentence_parts)
}

fn execute_select_statement(
    sententence_vec: Vec<String>,
    route: &String,
) -> Result<(), MiniSQLError> {
    let select = new_select(sententence_vec)?;
    let file_iter = file::handler::new_file_iterator(route, &select.target_table)?;

    execute_select(&select, file_iter)?;
    Ok(())
}

fn decode_select(sentence_parts: Vec<String>) -> Result<Select, MiniSQLError> {
    let fields: Vec<String> = Vec::new();
    let condition: Vec<String> = Vec::new();
    let order_by: Vec<String> = Vec::new();
    let from: Vec<String> = Vec::new();

    let fields_to_select = match_fields_select(sentence_parts, fields, condition, from, order_by)?;

    let table = validate_select_fields(fields_to_select.target_table, &fields_to_select.order)?;

    Ok(Select {
        target_table: table,
        condition: fields_to_select.condition,
        mapped_fields: fields_to_select.mapped_fields,
        order: fields_to_select.order,
    })
}

fn match_fields_select(
    sentence_parts: Vec<String>,
    mut fields: Vec<String>,
    mut condition: Vec<String>,
    mut from: Vec<String>,
    mut order_by: Vec<String>,
) -> Result<FieldsToSelect, MiniSQLError> {
    let mut base = "";
    for part in &sentence_parts {
        match part.as_str() {
            "SELECT" => {
                base = "fields";
                continue;
            }
            "WHERE" => {
                base = "condition";
                continue;
            }
            "FROM" => {
                base = "from";
                continue;
            }
            "ORDER" => {
                base = "order";
                continue;
            }
            "BY" => {
                if base == "order" {
                    base = "order by"
                }
                continue;
            }
            _ => (), // los anteriores casos son los que separan segmentos, aca cae todo lo demas
        }

        match base {
            "fields" => fields.push(part.to_string()),
            "condition" => condition.push(part.to_string()),
            "from" => from.push(part.to_string()),
            "order by" => order_by.push(part.to_string()),
            _ => {
                return Err(MiniSQLError::InvalidSyntax(format!(
                    "Invalid sentence: {} ",
                    sentence_parts.join(" ")
                )))
            }
        }
    }

    Ok(FieldsToSelect {
        target_table: from,
        condition,
        mapped_fields: fields,
        order: order_by,
    })
}

fn validate_select_fields(
    mut from: Vec<String>,
    order_by: &[String],
) -> Result<String, MiniSQLError> {
    let table: String;

    if let Some(first) = from.pop() {
        table = first
    } else {
        return Err(MiniSQLError::InvalidTable(
            "no table was given ".to_string(),
        ));
    }

    if table.is_empty() {
        return Err(MiniSQLError::InvalidTable(
            "no table was given ".to_string(),
        ));
    }

    if !from.is_empty() {
        return Err(MiniSQLError::InvalidTable(
            "multiple references for table".to_string(),
        ));
    }

    if order_by.len() > 2 {
        return Err(MiniSQLError::InvalidTable(
            "multiple references for table".to_string(),
        ));
    }

    Ok(table)
}

fn execute_select(select: &Select, file_iter: BufReader<File>) -> Result<(), MiniSQLError> {
    let (file_iter, headers) = get_headers(file_iter);
    let requiered_fields = get_required_fields(&select.mapped_fields, &headers)?;
    let mapped_fields = add_all_fields(&headers);

    let mut response = apply_select_to_file(select, file_iter, &mapped_fields)?;
    response = order_response(response, &select.order, &mapped_fields)?;
    print_selected_registers(response, requiered_fields);

    Ok(())
}


fn apply_select_to_file(
    select: &Select,
    file_iter: BufReader<File>,
    mapped_fields: &HashMap<String, usize>,
) -> Result<Vec<Vec<String>>, MiniSQLError> {
    let mut response: Vec<Vec<String>> = vec![];

    for result in file_iter.lines() {
        let record = result?;
        let line = format_to_csv(record);
        let should_apply = sentences::conditions::get_query(
            &select.condition,
            0,
            select.condition.len(),
            mapped_fields,
            &line,
        )?;
        if should_apply {
            response.push(line)
        }
    }

    Ok(response)
}

fn print_selected_registers(response: Vec<Vec<String>>, requiered_fields: HashMap<String, usize>) {
    for register in response {
        let mut indices: Vec<usize> = requiered_fields.values().cloned().collect();
        indices.sort();

        // Imprimo los valores en el orden correcto
        let mut print_line: Vec<&str> = vec![];
        for index in indices {
            if let Some(value) = register.get(index) {
                print_line.push(value);
            }
        }
        let joined_line = print_line.join(", ");
        println!("{}", joined_line);
    }
}

fn order_response(
    response: Vec<Vec<String>>,
    order_by: &[String],
    mapped_fields: &HashMap<String, usize>,
) -> Result<Vec<Vec<String>>, MiniSQLError> {
    if order_by.is_empty() {
        return Ok(response);
    }

    let reference_field: &usize = if let Some(search) = order_by.first() {
        if let Some(field_index) = mapped_fields.get(search) {
            field_index
        } else {
            return Err(MiniSQLError::InvalidSyntax(format!(
                "Invalid field to order: {} ",
                search
            )));
        }
    } else {
        return Err(MiniSQLError::InvalidSyntax(format!(
            "unexpected error while trying to order: {} ",
            order_by.join(" ")
        )));
    };

    let order_style = get_order_style(order_by)?;

    if response.len() < 2 {
        // descartando este caso, puedo tomar el primero
        return Ok(response);
    }

    let (was_numeric, mut response) =
        order_numeric(response, reference_field, order_style, order_by)?;
    if was_numeric {
        return Ok(response);
    }

    response = order_literal(response, reference_field, order_style);
    Ok(response)
}

fn get_order_style(order_by: &[String]) -> Result<&str, MiniSQLError> {
    let mut order_style = "ASC";

    if let Some(order_type) = order_by.get(1) {
        if order_type == "DESC" {
            order_style = order_type;
        } else if order_type != "ASC" {
            return Err(MiniSQLError::InvalidSyntax(format!(
                "Invalid order type (must be ASC/DESC): {} ",
                order_type
            )));
        }
    } else {
        return Ok(order_style); // si no se envio, se toma ASC por defecto
    }

    Ok(order_style)
}

fn order_numeric(
    mut response: Vec<Vec<String>>,
    reference_field: &usize,
    order_style: &str,
    order_by: &[String],
) -> Result<(bool, Vec<Vec<String>>), MiniSQLError> {
    let is_numeric_field: bool = if let Some(line) = response.first() {
        if let Some(field) = line.get(*reference_field) {
            field.parse::<i32>().is_ok()
        } else {
            return Err(MiniSQLError::InvalidSyntax(format!(
                "unexpected error while trying to order: {} ",
                order_by.join(" ")
            )));
        }
    } else {
        return Err(MiniSQLError::InvalidSyntax(format!(
            "unexpected error while trying to order: {} ",
            order_by.join(" ")
        )));
    };

    if is_numeric_field {
        response.sort_by(|a, b| {
            let elem_a = a
                .get(*reference_field)
                .unwrap_or(&"".to_string())
                .parse::<i32>()
                .ok()
                .unwrap_or(0);
            let elem_b = b
                .get(*reference_field)
                .unwrap_or(&"".to_string())
                .parse::<i32>()
                .ok()
                .unwrap_or(0);
            if order_style == "ASC" {
                elem_a.cmp(&elem_b)
            } else {
                elem_b.cmp(&elem_a)
            }
        });
        Ok((true, response))
    } else {
        Ok((false, response))
    }
}

fn order_literal(
    mut response: Vec<Vec<String>>,
    reference_field: &usize,
    order_style: &str,
) -> Vec<Vec<String>> {
    response.sort_by(|a, b| {
        let default_a = "".to_string();
        let default_b = "".to_string();
        let elem_a = a.get(*reference_field).unwrap_or(&default_a);
        let elem_b = b.get(*reference_field).unwrap_or(&default_b);
        if order_style == "ASC" {
            elem_a.cmp(elem_b)
        } else {
            elem_b.cmp(elem_a)
        }
    });
    response
}


