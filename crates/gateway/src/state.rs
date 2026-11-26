use std::collections::HashMap;
use std::sync::Mutex;

use chat_auth::AuthService;
use chat_common::snowflake::SnowflakeGenerator;
use chat_service::ChatService;

use crate::link_preview::LinkPreview;
use crate::media::MediaStore;

pub struct AppState {
    pub auth: AuthService,
    pub chat: ChatService,
    pub nats: async_nats::Client,
    pub media: MediaStore,
    pub media_ids: SnowflakeGenerator,
    pub link_preview_client: reqwest::Client,
    // Unbounded for now, like everything else in this Phase 1 state — fine
    // for a single-process dev deployment; a real deployment would want a
    // size cap/TTL (or move this to Redis) before it grows without limit.
    pub link_previews: Mutex<HashMap<String, LinkPreview>>,
}
