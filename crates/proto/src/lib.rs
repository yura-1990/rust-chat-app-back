pub mod chat {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/chat.v1.rs"));
    }
}
