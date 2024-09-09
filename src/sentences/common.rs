use crate::errors::apperrors::MiniSQLError;
use std::collections::HashMap;

pub fn analyze_condition(
    condition: &[String],
    start: usize,
    end: usize,
    indexes: &HashMap<String, usize>,
    line: &[String],
) -> Result<bool, MiniSQLError> {
    match end - start {
        0 => execute_unary_condition(condition, start, end),
        2 => execute_binary_condition(condition, start, end, line, indexes),
        _ => {
            let broken_query_part = &condition[start..end];
            Err(MiniSQLError::InvalidSyntax(format!(
                "program was unable to execute comparison between: {} ",
                broken_query_part.join(" ")
            )))
        }
    }
}

fn execute_unary_condition(
    condition: &[String],
    start: usize,
    end: usize,
) -> Result<bool, MiniSQLError> {
    if let Some(part) = condition.get(start) {
        match part.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => {
                let broken_query_part = &condition[start..end];
                Err(MiniSQLError::InvalidSyntax(format!(
                    "program was unable to parse query on alone condition: {} ",
                    broken_query_part.join(" ")
                )))
            }
        }
    } else {
        let broken_query_part = &condition[start..end];
        Err(MiniSQLError::InvalidSyntax(format!(
            "program found unexpected error while parsing query on condition: {} ",
            broken_query_part.join(" ")
        )))
    }
}

fn execute_binary_condition(
    condition: &[String],
    start: usize,
    end: usize,
    line: &[String],
    indexes: &HashMap<String, usize>,
) -> Result<bool, MiniSQLError> {
    let val1 = get_cond_value(condition, start, indexes, line)?;
    let val2 = get_cond_value(condition, start + 2, indexes, line)?;
    let val1_numeric = val1.parse::<i32>();
    let val2_numeric = val2.parse::<i32>();

    if val1_numeric.is_ok() && val2_numeric.is_ok() {
        if let Some(operator) = condition.get(start + 1) {
            let num1 = match val1_numeric {
                Ok(val1) => val1,
                Err(_) => {
                    let broken_query_part = &condition[start..end];
                    return Err(MiniSQLError::InvalidSyntax(format!(
                        "program found unexpected error while parsing number to int: {} ",
                        broken_query_part.join(" ")
                    )));
                }
            };

            let num2 = match val2_numeric {
                Ok(val2) => val2,
                Err(_) => {
                    let broken_query_part = &condition[start..end];
                    return Err(MiniSQLError::InvalidSyntax(format!(
                        "program found unexpected error while parsing number to int: {} ",
                        broken_query_part.join(" ")
                    )));
                }
            };

            match operator.as_str() {
                "=" => Ok(num1 == num2),
                "!=" | "<>" => Ok(num1 != num2),
                ">" => Ok(num1 > num2),
                ">=" => Ok(num1 >= num2),
                "<=" => Ok(num1 <= num2),
                "<" => Ok(num1 < num2),
                _ => Err(MiniSQLError::InvalidSyntax(format!(
                    "invalid operand for comparison: {} ",
                    operator
                ))),
            }
        } else {
            let broken_query_part = &condition[start..end];
            Err(MiniSQLError::InvalidSyntax(format!(
                "program found unexpected error while parsing query on condition: {} ",
                broken_query_part.join(" ")
            )))
        }
    } else if val1_numeric.is_err() && val2_numeric.is_err() {
        if let Some(operator) = condition.get(start + 1) {
            match operator.as_str() {
                "=" => return Ok(val1 == val2),
                "!=" | "<>" => return Ok(val1 != val2),
                ">" => return Ok(val1 > val2),
                ">=" => return Ok(val1 >= val2),
                "<=" => return Ok(val1 <= val2),
                "<" => return Ok(val1 < val2),
                _ => {
                    return Err(MiniSQLError::InvalidSyntax(format!(
                        "invalid operand for comparison: {} ",
                        operator
                    )))
                }
            }
        } else {
            let broken_query_part = &condition[start..end];
            return Err(MiniSQLError::InvalidSyntax(format!(
                "program found unexpected error while parsing query on condition: {} ",
                broken_query_part.join(" ")
            )));
        }
    } else {
        return Err(MiniSQLError::InvalidSyntax(
            "invalid type comparison, number and string literal".to_string(),
        ));
    }
}

fn get_cond_value(
    condition: &[String],
    index: usize,
    indexes: &HashMap<String, usize>,
    line: &[String],
) -> Result<String, MiniSQLError> {
    if let Some(part) = condition.get(index) {
        if let Some(index) = indexes.get(part) {
            if let Some(line_value) = line.get(*index) {
                Ok(line_value.to_string())
            } else {
                Err(MiniSQLError::InvalidSyntax(
                    "program found unexpected error while replacing value from condition"
                        .to_string(),
                ))
            }
        } else {
            return Ok(part.as_str().replace("'", ""));
        }
    } else {
        Err(MiniSQLError::InvalidSyntax(
            "program found unexpected error while replacing value from condition".to_string(),
        ))
    }
}

#[cfg(test)]
mod test_unary {
    use super::*;
    use crate::errors::apperrors::MiniSQLError;

    #[test]
    fn test_execute_unary_condition_true() {
        let condition = vec!["true".to_string()];
        let result = execute_unary_condition(&condition, 0, 1);
        assert_eq!(result, Ok(true));
    }
    #[test]
    fn test_execute_unary_condition_false() {
        let condition = vec!["false".to_string()];
        let result = execute_unary_condition(&condition, 0, 1);
        assert_eq!(result, Ok(false));
    }

    #[test]
    fn test_execute_unary_condition_invalid() {
        let condition = vec!["nombre".to_string()];
        let result = execute_unary_condition(&condition, 0, 1);
        assert_eq!(
            result,
            Err(MiniSQLError::InvalidSyntax(format!(
                "program was unable to parse query on alone condition: {} ",
                &condition[0..1].join(" ")
            )))
        );
    }

    #[test]
    fn test_execute_unary_condition_empty() {
        let condition = vec![];
        let result = execute_unary_condition(&condition, 0, 0);
        assert_eq!(
            result,
            Err(MiniSQLError::InvalidSyntax(format!(
                "program found unexpected error while parsing query on condition: {} ",
                &condition[0..0].join(" ")
            )))
        );
    }
}

#[cfg(test)]
mod test_binary {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_execute_binary_condition_hardcoded_equal_numbers() {
        let condition = vec!["1".to_string(), "=".to_string(), "1".to_string()];
        let indexes = HashMap::new();
        let line = vec![];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(true));
    }

    #[test]
    fn test_execute_binary_condition_hardcoded_not_equal_numbers() {
        let condition = vec!["1".to_string(), "=".to_string(), "2".to_string()];
        let indexes = HashMap::new();
        let line = vec![];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(false));
    }

    #[test]
    fn test_execute_binary_condition_hardcoded_equal_strings() {
        let condition = vec!["'Pepe'".to_string(), "=".to_string(), "'Pepe'".to_string()];
        let indexes = HashMap::new();
        let line = vec![];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(true));
    }

    #[test]
    fn test_execute_binary_condition_hardcoded_not_equal_strings() {
        let condition = vec!["'Pepe'".to_string(), "=".to_string(), "'Pablo'".to_string()];
        let indexes = HashMap::new();
        let line = vec![];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(false));
    }

    #[test]
    fn test_execute_binary_condition_equal_values() {
        let condition = vec!["Nombre".to_string(), "=".to_string(), "'Pepe'".to_string()];
        let indexes = HashMap::from([
            ("ID".to_string(), 0),
            ("Nombre".to_string(), 1),
            ("Edad".to_string(), 2),
        ]);
        let line: &[String] = &vec!["1002".to_string(), "Pepe".to_string(), "19".to_string()];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(true));
    }

    #[test]
    fn test_execute_binary_condition_equal_values_number() {
        let condition = vec!["Edad".to_string(), "=".to_string(), "19".to_string()];
        let indexes = HashMap::from([
            ("ID".to_string(), 0),
            ("Nombre".to_string(), 1),
            ("Edad".to_string(), 2),
        ]);
        let line: &[String] = &vec!["1002".to_string(), "Pepe".to_string(), "19".to_string()];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(true));
    }

    #[test]
    fn test_execute_binary_condition_row_equals_row() {
        let condition = vec![
            "Nombre".to_string(),
            "=".to_string(),
            "Apellido".to_string(),
        ];
        let indexes = HashMap::from([
            ("ID".to_string(), 0),
            ("Nombre".to_string(), 1),
            ("Apellido".to_string(), 2),
            ("Edad".to_string(), 3),
        ]);
        let line: &[String] = &vec![
            "1002".to_string(),
            "Natalia".to_string(),
            "Natalia".to_string(),
            "19".to_string(),
        ];
        let result = execute_binary_condition(&condition, 0, 2, &line, &indexes);
        assert_eq!(result, Ok(true));
    }
}
