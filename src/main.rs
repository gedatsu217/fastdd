fn main() {
    let arg_data = fastdd::arg_parse();
    let result = fastdd::execute_dd(&arg_data);
    match result {
        Ok(bytes_copied) => println!("Successfully copied {} bytes.", bytes_copied),
        Err(e) => eprintln!("Error during copy: {}", e),
    }
}