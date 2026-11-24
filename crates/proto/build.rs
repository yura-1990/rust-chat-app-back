fn main() {
    println!("cargo:rerun-if-changed=../../proto/chat.proto");
    prost_build::compile_protos(&["../../proto/chat.proto"], &["../../proto"])
        .expect("failed to compile chat.proto");
}
