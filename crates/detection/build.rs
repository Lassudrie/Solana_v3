use tonic_build::configure;

fn main() {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        unsafe {
            std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
        }
    }

    configure()
        .compile_protos(
            &["proto/shared.proto", "proto/shredstream.proto"],
            &["proto"],
        )
        .expect("compile shredstream protos");
}
