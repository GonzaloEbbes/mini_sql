use std::fs;
use std::io::{self,  Write};

const CLIENTES_DIR: &str = "data/tables/clientes.csv";
fn duplicate_file(original_path: &str,  new_path: &str) -> io::Result<()> {
    let content = fs::read(original_path)?;

    let mut new_file = fs::File::create(new_path)?;
    new_file.write_all(&content)?;

    Ok(())
}


#[cfg(test)]
mod tests_select {
    use crate::{duplicate_file,  CLIENTES_DIR};

    #[test]
    fn test_select_missing_from() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * WHERE id = 5")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [no table was given ]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_select_invalid_table_name() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM nada")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [Unable to open file at data/tables/nada]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_select_all_fields() {
        let expected: Vec<&str> = vec![
            "101, mario, hernandez, mario@email.com, 5551234567\n", 
            "102, laura, ortega, laura@email.com, 5559876543\n", 
            "103, javier, diaz, javier@email.com, 5551122334\n", 
            "104, carla, rivera, carla@email.com, 5556677889\n", 
            "105, andres, ruiz, andres@email.com, 5552233445\n", 
            "106, lucia, garcia, lucia@email.com, 5553344556\n", 
            "107, fernando, moreno, fernando@email.com, 5554455667\n", 
            "108, sofia, gonzalez, sofia@email.com, 5555566778\n", 
            "109, rafael, diaz, rafael@email.com, 5556677881\n", 
            "110, paula, vera, paula@email.com, 5557788992\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM clientes")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]
    fn test_select_all_fields_where() {
        let expected: Vec<&str> = vec![
            "103, javier, diaz, javier@email.com, 5551122334\n",  
            "109, rafael, diaz, rafael@email.com, 5556677881\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM clientes WHERE apellido = 'diaz'")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_some_fields_where() {
        let expected: Vec<&str> = vec![
            "103, javier@email.com\n",  
            "109, rafael@email.com\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz'")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        println!("{:?}", stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_some_fields() {
        let expected: Vec<&str> = vec![
            "mario@email.com, 5551234567\n", 
            "laura@email.com, 5559876543\n", 
            "javier@email.com, 5551122334\n", 
            "carla@email.com, 5556677889\n", 
            "andres@email.com, 5552233445\n", 
            "lucia@email.com, 5553344556\n", 
            "fernando@email.com, 5554455667\n", 
            "sofia@email.com, 5555566778\n", 
            "rafael@email.com, 5556677881\n", 
            "paula@email.com, 5557788992\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, telefono FROM clientes")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        println!("{:?}", stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    

}
