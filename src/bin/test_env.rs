fn main() {
    for (key, value) in std::env::vars() {
        if key.to_lowercase().starts_with("tss") {
            println!("{} = {}", key, value);
        }
    }
}
