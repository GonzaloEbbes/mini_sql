use std::fs;
use std::io::{self,  Write};

const CLIENTES_DIR: &str = "data/tables/clientes.csv";
const CLIENTES2_DIR: &str = "data/tables/clientes2.csv";
const ORDENES_DIR: &str = "data/tables/ordenes.csv";
const PERSONAS_DIR: &str = "data/tables/personas.csv";
fn duplicate_temp_file(original_path: &str,  new_path: &str, folder: &str, file: &str) -> io::Result<()> {
    fs::create_dir(format!("{}/{}", new_path, folder))?;
    let content = fs::read(original_path)?;
    let mut new_file = fs::File::create(format!("{}/{}/{}", new_path, folder, file))?;
    new_file.write_all(&content)?;

    Ok(())
}

fn delete_file(dir_name: &str) -> io::Result<()>{
    return fs::remove_dir_all(dir_name)
}


#[cfg(test)]
mod tests_select {

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

        
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_some_fields_where_and() {
        let expected: Vec<&str> = vec![
            "103, javier@email.com\n",  
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz' AND id_cliente < 105")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_some_fields_where_or() {
        let expected: Vec<&str> = vec![
            "101, mario@email.com\n", 
            "102, laura@email.com\n", 
            "103, javier@email.com\n",  
            "104, carla@email.com\n", 
            "105, andres@email.com\n", 
            "109, rafael@email.com\n",  
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz' OR id_cliente <= 105")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_all_fields_where_not() {
        let expected: Vec<&str> = vec![
            "101, mario, hernandez, mario@email.com, 5551234567\n", 
            "102, laura, ortega, laura@email.com, 5559876543\n", 
            "103, javier, diaz, javier@email.com, 5551122334\n", 
            "105, andres, ruiz, andres@email.com, 5552233445\n", 
            "106, lucia, garcia, lucia@email.com, 5553344556\n", 
            "107, fernando, moreno, fernando@email.com, 5554455667\n", 
            "108, sofia, gonzalez, sofia@email.com, 5555566778\n", 
            "109, rafael, diaz, rafael@email.com, 5556677881\n", 
            "110, paula, vera, paula@email.com, 5557788992\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM clientes WHERE NOT email = 'carla@email.com'")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_ordering_asc_default() {
        let expected: Vec<&str> = vec![
            "105, andres@email.com\n", 
            "104, carla@email.com\n", 
            "103, javier@email.com\n",  
            "102, laura@email.com\n", 
            "101, mario@email.com\n", 
            "109, rafael@email.com\n",  
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz' OR id_cliente <= 105 ORDER BY nombre")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_ordering_asc_explicit() {
        let expected: Vec<&str> = vec![
            "105, andres@email.com\n", 
            "104, carla@email.com\n", 
            "103, javier@email.com\n",  
            "102, laura@email.com\n", 
            "101, mario@email.com\n", 
            "109, rafael@email.com\n",  
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz' OR id_cliente <= 105 ORDER BY nombre ASC")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_ordering_desc() {
        let expected: Vec<&str> = vec![
            "109, rafael@email.com\n",  
            "101, mario@email.com\n", 
            "102, laura@email.com\n", 
            "103, javier@email.com\n", 
            "104, carla@email.com\n",  
            "105, andres@email.com\n", 
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz' OR id_cliente <= 105 ORDER BY nombre DESC")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_order_numeric() {
        let expected: Vec<&str> = vec![
            "103, javier, diaz, javier@email.com, 5551122334\n",
            "101, mario, hernandez, mario@email.com, 5551234567\n",
            "105, andres, ruiz, andres@email.com, 5552233445\n",
            "106, lucia, garcia, lucia@email.com, 5553344556\n",
            "107, fernando, moreno, fernando@email.com, 5554455667\n",
            "108, sofia, gonzalez, sofia@email.com, 5555566778\n",
            "109, rafael, diaz, rafael@email.com, 5556677881\n",
            "110, paula, vera, paula@email.com, 5557788992\n",
            "102, laura, ortega, laura@email.com, 5559876543\n"
        ];


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM clientes WHERE NOT email = 'carla@email.com'  ORDER BY telefono ASC ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_ordering_numeric_desc() {
        let expected: Vec<&str> = vec![
            "109, rafael@email.com\n",  
            "105, andres@email.com\n", 
            "104, carla@email.com\n", 
            "103, javier@email.com\n",  
            "102, laura@email.com\n", 
            "101, mario@email.com\n", 
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT email, id_cliente FROM clientes WHERE apellido = 'diaz' OR id_cliente <= 105 ORDER BY id_cliente DESC")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_example_1() {
        let expected: Vec<&str> = vec![
            "102, 2, Teléfono\n",
            "104, 3, Teclado\n",
            "105, 4, Mouse\n",
            "107, 6, Altavoces\n",
            "110, 6, Teléfono\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT id, producto, id_cliente FROM ordenes WHERE cantidad > 1;")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_example_2() {
        let expected: Vec<&str> = vec![
            "5, José, jose.lopez@email.com\n",
            "2, Ana, ana.lopez@email.com\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT id, nombre, email FROM clientes2 WHERE apellido = 'López' ORDER BY email DESC;")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_mutiple_conditions() {
        let expected: Vec<&str> = vec![
            "104, 3, Teclado, 1\n",
            "104, 3, Teclado, 4\n",
            "107, 6, Altavoces, 1\n",
            "110, 6, Teléfono, 2\n"

        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM ordenes WHERE id_cliente = 6 AND cantidad <= 2 OR id_cliente = 3")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_mutiple_conditions_2() {
        let expected: Vec<&str> = vec![
            "1, Ingeniería, 71, 72, Introducción a la Ingeniería, 4\n",
            "2, Física, 71, 74, Física Aplicada I, 5\n",
            "3, Matemática, 65, 76, Álgebra Lineal, 6\n",
            "5, Bioquímica, 80, 77, Bioquímica Avanzada, 7\n"
        ]; 


        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM materias WHERE NOT nombre = 'Desarrollo' AND departamento = 71 OR codigo > 75")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }

    #[test]  
    fn test_select_parenthesis(){
        let expected: Vec<&str> = vec![ 
            "103, javier, diaz, javier@email.com, 5551122334\n", 
            "109, rafael, diaz, rafael@email.com, 5556677881\n"
        ]; 

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("SELECT * FROM clientes WHERE apellido = 'diaz' AND ( id_cliente < 105 OR id_cliente = 109 )")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        assert_eq!(stdout,  expected.concat());
        assert!(stderr.is_empty());
    }
}

mod test_update {
    use crate::{delete_file, duplicate_temp_file, CLIENTES_DIR, CLIENTES2_DIR};

    #[test]
    fn test_update_missing_table() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("UPDATE SET email = 'mrodriguez@hotmail.com' WHERE id = 4 ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [no table was given ]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_update_invalid_table_name() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("UPDATE clientecitos SET email = 'mrodriguez@hotmail.com' WHERE id = 4 ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [Unable to open file at data/tables/clientecitos]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_basic_update(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("UPDATE clientes SET email = 'mrodriguez@hotmail.com' WHERE id_cliente = 104")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,mrodriguez@hotmail.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n", 
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_double_update(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("UPDATE clientes SET email = 'pepe@hotmail.com', nombre = 'pepe' WHERE id_cliente > 108 ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n", 
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,pepe,diaz,pepe@hotmail.com,5556677881\n", 
            "110,pepe,vera,pepe@hotmail.com,5557788992\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_basic_update_last_field(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("UPDATE clientes SET telefono = 123123123 WHERE id_cliente = 104")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,123123123\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n", 
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_basic_update_where_last_field(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("UPDATE clientes SET nombre = 'pepe' WHERE telefono = 5553344556")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,pepe,garcia,lucia@email.com,5553344556\n", 
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_update_example_3(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES2_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes2.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("UPDATE clientes2 SET email = 'mrodriguez@hotmail.com' WHERE id = 4")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id,nombre,apellido,email\n",
            "1,Juan,Pérez,juan.perez@email.com\n",
            "2,Ana,López,ana.lopez@email.com\n",
            "3,Carlos,Gómez,carlos.gomez@email.com\n",
            "4,María,Rodríguez,mrodriguez@hotmail.com\n",
            "5,José,López,jose.lopez@email.com\n",
            "6,Laura,Fernández,laura.fernandez@email.com\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes2.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

}

mod test_delete {
    use crate::{delete_file, duplicate_temp_file, CLIENTES_DIR};

    #[test]
    fn test_delete_missing_table() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("DELETE FROM WHERE id_cliente = 107")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [no table was given ]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_delete_invalid_table_name() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("DELETE FROM clientecitos WHERE id_cliente = 107")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [Unable to open file at data/tables/clientecitos]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_basic_delete(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("DELETE FROM clientes WHERE id_cliente = 107")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_double_delete(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("DELETE FROM clientes WHERE id_cliente > 108 ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n", 
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_basic_delete_where_last_field(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("DELETE FROM clientes WHERE telefono = 5553344556")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

}

mod test_insert {
    use crate::{delete_file, duplicate_temp_file, CLIENTES_DIR, ORDENES_DIR, PERSONAS_DIR};

    #[test]
    fn test_insert_missing_table() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("INSERT INTO (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3) ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [no table was given ]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_insert_invalid_table_name() {
        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg("data/tables")
        .arg("INSERT INTO clientecitos (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3) ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected_error = "[INVALID_TABLE]: [Unable to open file at data/tables/clientecitos]\n";

        assert_eq!(stderr,  expected_error);
        assert!(stdout.is_empty());
    }

    #[test]
    fn test_basic_insert(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("INSERT INTO clientes (id_cliente, nombre, apellido, email) VALUES (111, 'pepe', 'garcia', 'pepe@email.com') ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n",
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n",
            "111,pepe,garcia,pepe@email.com,\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_full_insert(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("INSERT INTO clientes (id_cliente, nombre, apellido, email, telefono ) VALUES (111, 'pepe', 'garcia', 'pepe@email.com', 5551234990) ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n",
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n",
            "111,pepe,garcia,pepe@email.com,5551234990\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_double_insert(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(CLIENTES_DIR, "tests",&format!("temp-{}", clean_thread_id), "clientes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("INSERT INTO clientes (id_cliente, nombre, apellido, email) VALUES (111, 'pepe', 'garcia', 'pepe@email.com'), (112, 'carlos', 'rodriguez', 'carlos@email.com') ")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_cliente,nombre,apellido,email,telefono\n", 
            "101,mario,hernandez,mario@email.com,5551234567\n", 
            "102,laura,ortega,laura@email.com,5559876543\n", 
            "103,javier,diaz,javier@email.com,5551122334\n", 
            "104,carla,rivera,carla@email.com,5556677889\n", 
            "105,andres,ruiz,andres@email.com,5552233445\n", 
            "106,lucia,garcia,lucia@email.com,5553344556\n",
            "107,fernando,moreno,fernando@email.com,5554455667\n", 
            "108,sofia,gonzalez,sofia@email.com,5555566778\n", 
            "109,rafael,diaz,rafael@email.com,5556677881\n", 
            "110,paula,vera,paula@email.com,5557788992\n",
            "111,pepe,garcia,pepe@email.com,\n",
            "112,carlos,rodriguez,carlos@email.com,\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/clientes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_insert_example_4(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(ORDENES_DIR, "tests",&format!("temp-{}", clean_thread_id), "ordenes.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("INSERT INTO ordenes (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3);")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![ 
            "id,id_cliente,producto,cantidad\n",
            "101,1,Laptop,1\n",
            "103,1,Monitor,1\n",
            "102,2,Teléfono,2\n",
            "104,3,Teclado,1\n",
            "104,3,Teclado,4\n",
            "105,4,Mouse,2\n",
            "106,5,Impresora,1\n",
            "107,6,Altavoces,1\n",
            "107,6,Altavoces,4\n",
            "108,4,Auriculares,1\n",
            "109,5,Laptop,1\n",
            "110,6,Teléfono,2\n",
            "111,6,Laptop,3\n",
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/ordenes.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

    #[test]
    fn test_insert_missing_fields(){
        let thread_id = std::thread::current().id();
        let thread_id_str = format!("{:?}", thread_id);
        let clean_thread_id = thread_id_str.replace("ThreadId(", "").replace(")", "");
        let mut ok = duplicate_temp_file(PERSONAS_DIR, "tests",&format!("temp-{}", clean_thread_id), "personas.csv");
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not duplicate file\n");
                assert_eq!(false, true)
            }
        }

        let output = std::process::Command::new("./target/debug/mini_sql")
        .arg(format!("tests/temp-{}", clean_thread_id))
        .arg("INSERT INTO personas (Nombre, Correo_electrónico) VALUES ('julian', 'julian@gmail.com');")
        .output()
        .expect("Failed to execute command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        let expected: Vec<&str> = vec![
            "id_persona,Nombre,Correo_electrónico,telefono,direccion\n",
            "1,carlos,carlos@gmail.com,1122334455,Calle Falsa 123\n",
            "2,ana,ana@gmail.com,1122335566,Calle Real 456\n",
            "3,martin,martin@hotmail.com,1133445566,Avenida Siempre Viva 789\n",
            ",julian,julian@gmail.com,,\n"
        ]; 

        let content = std::fs::read(format!("tests/temp-{}/personas.csv", clean_thread_id));
        ok = delete_file(&format!("tests/temp-{}", clean_thread_id));
        match ok {
            Ok(_) => (),
            Err(_) => {
                println!("FAIL: Could not delete file\n");
                assert_eq!(false, true)
            }
        }

        match content {
            Err(_) => assert_eq!(false, true),
            Ok(content) => {
                assert!(stderr.is_empty());
                assert!(stdout.is_empty());
                assert_eq!(String::from_utf8_lossy(&content), expected.concat())
            }
        }
    }

}
