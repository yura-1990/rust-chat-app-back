use chat_auth::AuthService;
use chat_common::snowflake::SnowflakeGenerator;
use chat_service::ChatService;

use crate::media::MediaStore;

pub struct AppState {
    pub auth: AuthService,
    pub chat: ChatService,
    pub nats: async_nats::Client,
    pub media: MediaStore,
    pub media_ids: SnowflakeGenerator,
}
