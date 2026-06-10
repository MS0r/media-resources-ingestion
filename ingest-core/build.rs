fn main() {
    tonic_build::compile_protos("proto/ingest.proto")
        .expect("Failed to compile ingest proto");
}
