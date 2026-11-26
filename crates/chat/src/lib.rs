mod error;

pub use error::ChatError;

use std::time::{SystemTime, UNIX_EPOCH};

use chat_common::snowflake::SnowflakeGenerator;
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct MediaRef {
    pub media_id: i64,
    pub kind: String,
    pub mime: String,
    pub name: Option<String>,
    pub size: i64,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ReplyPreviewRef {
    pub message_id: i64,
    pub author_id: i64,
    pub text: String,
    pub media_kind: Option<String>,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ForwardedFromRef {
    pub user_id: i64,
    pub username: String,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: i64,
    pub chat_id: i64,
    pub author_id: i64,
    pub text: String,
    pub created_at_ms: i64,
    pub media: Option<MediaRef>,
    pub reply_to_id: Option<i64>,
    pub reply_preview: Option<ReplyPreviewRef>,
    pub forwarded_from: Option<ForwardedFromRef>,
    pub edited_at_ms: Option<i64>,
    pub deleted_at_ms: Option<i64>,
    /// "text" for an ordinary authored message, "system" for a
    /// server-generated notice (e.g. "Alice created the group").
    pub kind: String,
    /// When this message was pinned, `None` if it isn't currently pinned.
    /// Chat-wide (not per-member, unlike `cleared_before_id`) — pinning is
    /// a shared action every member sees, the same way Telegram's is.
    pub pinned_at_ms: Option<i64>,
}

const REPLY_PREVIEW_MAX_CHARS: usize = 160;

fn truncate_preview(text: &str) -> String {
    if text.chars().count() <= REPLY_PREVIEW_MAX_CHARS {
        return text.to_string();
    }
    let truncated: String = text.chars().take(REPLY_PREVIEW_MAX_CHARS).collect();
    format!("{truncated}…")
}

#[derive(Debug, Clone)]
pub struct ChatSummary {
    pub chat_id: i64,
    /// `None` for a group/channel chat — there's no single "other" participant.
    pub other_user_id: Option<i64>,
    pub other_username: Option<String>,
    pub last_message: Option<LastMessage>,
    /// "Saved Messages" — a chat with no second member, used as a personal
    /// notes-to-self space (and, since it fans out over the same WS
    /// subscription as any other chat, syncs across every session the
    /// user has open). `other_user_id`/`other_username` are the caller's
    /// own identity in this case — there's no one else in the chat.
    pub is_self: bool,
    pub is_archived: bool,
    pub is_group: bool,
    /// A broadcast-style chat: only `owner`/`admin` members (see `my_role`)
    /// may post; everyone else is a read-only subscriber. Mutually
    /// exclusive with `is_group`.
    pub is_channel: bool,
    pub title: Option<String>,
    pub description: Option<String>,
    pub avatar_media_id: Option<i64>,
    pub member_count: i64,
    /// A channel's public `@handle`, if it has one set — `None` means
    /// invite-only (not joinable via `find_channel_by_handle`/`join_channel`).
    /// Always `None` for direct/self/group chats.
    pub handle: Option<String>,
    /// This caller's own `chat_members.role` ("owner" | "admin" | "member"),
    /// `None` for direct/self chats where role isn't tracked. Lets the
    /// frontend decide "can I post here" (for a channel) without a second
    /// round trip.
    pub my_role: Option<String>,
    /// Messages in this chat, not authored by the caller, with id greater
    /// than their `last_read_message_id` — computed server-side so it
    /// survives the caller being offline/disconnected when they arrived,
    /// unlike a purely client-local "increment while connected" counter.
    /// Always 0 for a self chat (every message in it is the caller's own).
    pub unread_count: i64,
}

#[derive(Debug, Clone)]
pub struct ChannelPreview {
    pub chat_id: i64,
    pub title: String,
    pub description: Option<String>,
    pub avatar_media_id: Option<i64>,
    pub member_count: i64,
    pub handle: String,
}

#[derive(Debug, Clone)]
pub struct LastMessage {
    pub text: String,
    pub created_at_ms: i64,
    pub media_kind: Option<String>,
    pub deleted: bool,
}

pub struct ChatService {
    pool: PgPool,
    ids: SnowflakeGenerator,
}

impl ChatService {
    pub fn new(pool: PgPool, ids: SnowflakeGenerator) -> Self {
        Self { pool, ids }
    }

    /// Returns the existing direct chat between the two users, creating one
    /// if it doesn't exist yet. `user_a == user_b` redirects to Saved
    /// Messages instead — a "direct chat with yourself" isn't a second
    /// chat kind worth having, it's exactly what Saved Messages already is.
    pub async fn get_or_create_direct_chat(&self, user_a: i64, user_b: i64) -> Result<i64, ChatError> {
        if user_a == user_b {
            return self.get_or_create_saved_messages(user_a).await;
        }
        if let Some(existing) = self.find_direct_chat(user_a, user_b).await? {
            return Ok(existing);
        }

        let chat_id = self.ids.next_id();
        let mut tx = self.pool.begin().await?;
        sqlx::query("insert into chats (id, kind) values ($1, 'direct')")
            .bind(chat_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("insert into chat_members (chat_id, user_id) values ($1, $2), ($1, $3)")
            .bind(chat_id)
            .bind(user_a)
            .bind(user_b)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(chat_id)
    }

    /// Returns the caller's "Saved Messages" chat, creating it the first
    /// time it's needed. Exactly one member (no "other" participant) — a
    /// personal space for notes-to-self that also happens to sync across
    /// every session the user has open, for free, via the same per-chat
    /// WS fan-out every other chat already uses.
    pub async fn get_or_create_saved_messages(&self, user_id: i64) -> Result<i64, ChatError> {
        let existing: Option<(i64,)> = sqlx::query_as(
            "select c.id from chats c
             join chat_members m on m.chat_id = c.id and m.user_id = $1 and m.left_at_ms is null
             where c.kind = 'self'
             limit 1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((chat_id,)) = existing {
            return Ok(chat_id);
        }

        let chat_id = self.ids.next_id();
        let mut tx = self.pool.begin().await?;
        sqlx::query("insert into chats (id, kind) values ($1, 'self')")
            .bind(chat_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("insert into chat_members (chat_id, user_id) values ($1, $2)")
            .bind(chat_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(chat_id)
    }

    /// Creates a new group chat: the creator becomes `role = 'owner'`,
    /// everyone else in `member_ids` joins as a plain `'member'`. Unlike
    /// direct chats, there's no reuse-detection — creating a group always
    /// makes a brand new chat.
    pub async fn create_group_chat(
        &self,
        creator_id: i64,
        title: &str,
        description: Option<&str>,
        avatar_media_id: Option<i64>,
        member_ids: &[i64],
    ) -> Result<i64, ChatError> {
        let chat_id = self.ids.next_id();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "insert into chats (id, kind, title, description, avatar_media_id, created_by)
             values ($1, 'group', $2, $3, $4, $5)",
        )
        .bind(chat_id)
        .bind(title)
        .bind(description)
        .bind(avatar_media_id)
        .bind(creator_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query("insert into chat_members (chat_id, user_id, role) values ($1, $2, 'owner')")
            .bind(chat_id)
            .bind(creator_id)
            .execute(&mut *tx)
            .await?;
        for member_id in member_ids {
            sqlx::query("insert into chat_members (chat_id, user_id, role) values ($1, $2, 'member')")
                .bind(chat_id)
                .bind(member_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(chat_id)
    }

    /// Creates a new channel: the creator becomes `role = 'owner'`, every id
    /// in `member_ids` joins as a plain `'member'` (subscriber) — identical
    /// shape to `create_group_chat`, except `kind = 'channel'`, an optional
    /// public `handle` (validated the same way a user's own `@handle` is),
    /// and `member_ids` may legitimately be empty (a channel can start with
    /// zero subscribers, unlike a group).
    pub async fn create_channel_chat(
        &self,
        creator_id: i64,
        title: &str,
        description: Option<&str>,
        avatar_media_id: Option<i64>,
        handle: Option<&str>,
        member_ids: &[i64],
    ) -> Result<i64, ChatError> {
        if let Some(handle) = handle
            && !is_valid_handle(handle)
        {
            return Err(ChatError::InvalidHandle);
        }

        let chat_id = self.ids.next_id();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "insert into chats (id, kind, title, description, avatar_media_id, created_by, handle)
             values ($1, 'channel', $2, $3, $4, $5, $6)",
        )
        .bind(chat_id)
        .bind(title)
        .bind(description)
        .bind(avatar_media_id)
        .bind(creator_id)
        .bind(handle)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            if is_unique_violation(&err) {
                ChatError::HandleTaken
            } else {
                ChatError::Database(err)
            }
        })?;

        sqlx::query("insert into chat_members (chat_id, user_id, role) values ($1, $2, 'owner')")
            .bind(chat_id)
            .bind(creator_id)
            .execute(&mut *tx)
            .await?;
        for member_id in member_ids {
            sqlx::query("insert into chat_members (chat_id, user_id, role) values ($1, $2, 'member')")
                .bind(chat_id)
                .bind(member_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(chat_id)
    }

    /// Owner-only: sets, changes, or (with `None`) clears a channel's public
    /// `@handle` after creation. Same validate-then-unique-violation-catch
    /// shape as `create_channel_chat`/`AuthService::set_handle`.
    pub async fn set_channel_handle(
        &self,
        chat_id: i64,
        acting_user_id: i64,
        handle: Option<&str>,
    ) -> Result<(), ChatError> {
        if self.member_role(chat_id, acting_user_id).await?.as_deref() != Some("owner") {
            return Err(ChatError::NotOwner);
        }
        if let Some(handle) = handle
            && !is_valid_handle(handle)
        {
            return Err(ChatError::InvalidHandle);
        }
        sqlx::query("update chats set handle = $1 where id = $2")
            .bind(handle)
            .bind(chat_id)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                if is_unique_violation(&err) {
                    ChatError::HandleTaken
                } else {
                    ChatError::Database(err)
                }
            })?;
        Ok(())
    }

    /// This caller's own role in a chat ("owner" | "admin" | "member"), or
    /// `None` if they're not an active member. Used both for permission
    /// checks (promote/demote, channel send-gate) and to populate
    /// `ChatSummary::my_role`.
    pub async fn member_role(&self, chat_id: i64, user_id: i64) -> Result<Option<String>, ChatError> {
        let row: Option<(String,)> = sqlx::query_as(
            "select role from chat_members where chat_id = $1 and user_id = $2 and left_at_ms is null",
        )
        .bind(chat_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(role,)| role))
    }

    /// `chat_members.kind` isn't a thing — this is `chats.kind` for a single
    /// chat, used by the channel send-permission gate (`ws::handle_send_message`)
    /// alongside `member_role` to decide "is this a channel, and can this
    /// member post in it."
    pub async fn chat_kind(&self, chat_id: i64) -> Result<Option<String>, ChatError> {
        let row: Option<(String,)> = sqlx::query_as("select kind from chats where id = $1")
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(kind,)| kind))
    }

    /// Owner-only — promotes an existing member to `'admin'` (can post in a
    /// channel, same send permission as the owner). 404-equivalent `Ok(false)`
    /// if `target_user_id` isn't an active member.
    pub async fn promote_member(&self, chat_id: i64, acting_user_id: i64, target_user_id: i64) -> Result<bool, ChatError> {
        self.set_member_role(chat_id, acting_user_id, target_user_id, "admin").await
    }

    /// Owner-only — demotes an admin back to a plain `'member'`.
    pub async fn demote_member(&self, chat_id: i64, acting_user_id: i64, target_user_id: i64) -> Result<bool, ChatError> {
        self.set_member_role(chat_id, acting_user_id, target_user_id, "member").await
    }

    async fn set_member_role(
        &self,
        chat_id: i64,
        acting_user_id: i64,
        target_user_id: i64,
        role: &str,
    ) -> Result<bool, ChatError> {
        if self.member_role(chat_id, acting_user_id).await?.as_deref() != Some("owner") {
            return Err(ChatError::NotOwner);
        }
        let row: Option<(i64,)> = sqlx::query_as(
            "update chat_members set role = $1
             where chat_id = $2 and user_id = $3 and left_at_ms is null and role <> 'owner'
             returning chat_id",
        )
        .bind(role)
        .bind(chat_id)
        .bind(target_user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    /// Public lookup for a channel by its `@handle` — no membership
    /// required, only ever matches `kind = 'channel'` rows with a non-null
    /// `handle` (a channel without one set is invite-only and simply isn't
    /// found this way).
    pub async fn find_channel_by_handle(&self, handle: &str) -> Result<Option<ChannelPreview>, ChatError> {
        let row: Option<(i64, String, Option<String>, Option<i64>, i64, String)> = sqlx::query_as(
            "select c.id, c.title, c.description, c.avatar_media_id,
                    (select count(*) from chat_members gm where gm.chat_id = c.id and gm.left_at_ms is null) as member_count,
                    c.handle
             from chats c
             where c.kind = 'channel' and c.handle = $1",
        )
        .bind(handle)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(chat_id, title, description, avatar_media_id, member_count, handle)| ChannelPreview {
            chat_id,
            title,
            description,
            avatar_media_id,
            member_count,
            handle,
        }))
    }

    /// Subscribes `user_id` to a *public* channel (must have a non-null
    /// `handle`) as a plain `'member'`. Idempotent — already being a member
    /// is a no-op success, not an error, same spirit as
    /// `get_or_create_direct_chat`'s reuse-over-duplicate behavior. Returns
    /// `Ok(false)` if `chat_id` isn't a public channel at all.
    pub async fn join_channel(&self, chat_id: i64, user_id: i64) -> Result<bool, ChatError> {
        let is_public_channel: Option<(i32,)> =
            sqlx::query_as("select 1 from chats where id = $1 and kind = 'channel' and handle is not null")
                .bind(chat_id)
                .fetch_optional(&self.pool)
                .await?;
        if is_public_channel.is_none() {
            return Ok(false);
        }
        sqlx::query(
            "insert into chat_members (chat_id, user_id, role) values ($1, $2, 'member')
             on conflict (chat_id, user_id) do update set left_at_ms = null",
        )
        .bind(chat_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(true)
    }

    /// `(user_id, username, role)` for every active member of a group or
    /// channel — used by the group-info/channel-info panel. Not
    /// membership-checked itself; callers must `is_member`-gate this the
    /// same way every other chat-scoped read already is.
    pub async fn group_members(&self, chat_id: i64) -> Result<Vec<(i64, String, String)>, ChatError> {
        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "select u.id, u.username, cm.role
             from chat_members cm
             join users u on u.id = cm.user_id
             where cm.chat_id = $1 and cm.left_at_ms is null
             order by (cm.role = 'owner') desc, u.username",
        )
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    // Only matches chats where *both* sides are still active members —
    // once either has left (see leave_chat), messaging the same person
    // again starts a fresh chat rather than resurrecting the old one.
    async fn find_direct_chat(&self, user_a: i64, user_b: i64) -> Result<Option<i64>, ChatError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "select c.id from chats c
             join chat_members m1 on m1.chat_id = c.id and m1.user_id = $1 and m1.left_at_ms is null
             join chat_members m2 on m2.chat_id = c.id and m2.user_id = $2 and m2.left_at_ms is null
             where c.kind = 'direct'
             limit 1",
        )
        .bind(user_a)
        .bind(user_b)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id,)| id))
    }

    /// A member who has left (see `leave_chat`) no longer counts as one —
    /// they stop being able to send/receive in this chat or access its
    /// media, same as if they'd never joined.
    pub async fn is_member(&self, chat_id: i64, user_id: i64) -> Result<bool, ChatError> {
        let row: Option<(i32,)> = sqlx::query_as(
            "select 1 from chat_members where chat_id = $1 and user_id = $2 and left_at_ms is null",
        )
        .bind(chat_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn member_ids(&self, chat_id: i64) -> Result<Vec<i64>, ChatError> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            "select user_id from chat_members where chat_id = $1 and left_at_ms is null",
        )
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Every chat (direct, self, group, or channel) the user belongs to,
    /// newest-activity first, with the other participant's identity (or
    /// group/channel metadata) and last message preview — exactly what a
    /// chat-list sidebar needs in one round trip.
    pub async fn list_chats_for_user(&self, user_id: i64) -> Result<Vec<ChatSummary>, ChatError> {
        // A dedicated FromRow struct rather than a raw tuple — sqlx's tuple
        // FromRow impls only go up to a bounded arity, and this query is
        // already past the comfortable range of that once handle/my_role
        // were added; a named struct has no such limit and reads better at
        // this width besides.
        #[derive(sqlx::FromRow)]
        struct Row {
            chat_id: i64,
            other_user_id: Option<i64>,
            other_username: Option<String>,
            body: Option<String>,
            created_at_ms: Option<i64>,
            media_kind: Option<String>,
            deleted_at_ms: Option<i64>,
            is_self: bool,
            is_archived: bool,
            is_group: bool,
            is_channel: bool,
            title: Option<String>,
            description: Option<String>,
            avatar_media_id: Option<i64>,
            member_count: i64,
            handle: Option<String>,
            my_role: Option<String>,
            unread_count: i64,
        }
        // Unioned rather than one query with an outer-joined "other" member,
        // because a direct chat's "other" is a different row (other.user_id
        // <> $1), a self chat's "other" is the same row as "me", and a
        // group/channel chat has no single "other" at all — genuinely
        // different shapes of query, not one query with an optional
        // condition. Postgres doesn't allow an expression (only a bare
        // column name) in a UNION's own ORDER BY, so the union is wrapped in
        // a subquery and ordered from the outside instead, where it's just a
        // plain SELECT.
        let rows: Vec<Row> = sqlx::query_as(
            "select * from (
                select c.id, u.id, u.username, lm.body, lm.created_at_ms, lm.media_kind, lm.deleted_at_ms, false as is_self, me.archived_at_ms is not null as is_archived,
                       false as is_group, false as is_channel, null::text as title, null::text as description, u.active_avatar_media_id as avatar_media_id, 2::bigint as member_count,
                       null::text as handle, null::text as my_role,
                       (select count(*) from messages um where um.chat_id = c.id and um.author_id <> $1 and um.deleted_at_ms is null
                        and um.id > greatest(coalesce(me.last_read_message_id, 0), coalesce(me.cleared_before_id, 0))) as unread_count
                from chats c
                join chat_members me on me.chat_id = c.id and me.user_id = $1 and me.left_at_ms is null
                join chat_members other on other.chat_id = c.id and other.user_id <> $1
                join users u on u.id = other.user_id
                left join lateral (
                    select body, created_at_ms, media_kind, deleted_at_ms from messages m
                    where m.chat_id = c.id and m.id > coalesce(me.cleared_before_id, 0)
                    order by m.id desc limit 1
                ) lm on true
                where c.kind = 'direct'

                union all

                select c.id, u.id, u.username, lm.body, lm.created_at_ms, lm.media_kind, lm.deleted_at_ms, true as is_self, me.archived_at_ms is not null as is_archived,
                       false as is_group, false as is_channel, null::text as title, null::text as description, u.active_avatar_media_id as avatar_media_id, 1::bigint as member_count,
                       null::text as handle, null::text as my_role,
                       (select count(*) from messages um where um.chat_id = c.id and um.author_id <> $1 and um.deleted_at_ms is null
                        and um.id > greatest(coalesce(me.last_read_message_id, 0), coalesce(me.cleared_before_id, 0))) as unread_count
                from chats c
                join chat_members me on me.chat_id = c.id and me.user_id = $1 and me.left_at_ms is null
                join users u on u.id = me.user_id
                left join lateral (
                    select body, created_at_ms, media_kind, deleted_at_ms from messages m
                    where m.chat_id = c.id and m.id > coalesce(me.cleared_before_id, 0)
                    order by m.id desc limit 1
                ) lm on true
                where c.kind = 'self'

                union all

                select c.id, null::bigint, null::text, lm.body, lm.created_at_ms, lm.media_kind, lm.deleted_at_ms, false as is_self, me.archived_at_ms is not null as is_archived,
                       c.kind = 'group' as is_group, c.kind = 'channel' as is_channel, c.title, c.description, c.avatar_media_id,
                       (select count(*) from chat_members gm where gm.chat_id = c.id and gm.left_at_ms is null) as member_count,
                       c.handle, me.role as my_role,
                       (select count(*) from messages um where um.chat_id = c.id and um.author_id <> $1 and um.deleted_at_ms is null
                        and um.id > greatest(coalesce(me.last_read_message_id, 0), coalesce(me.cleared_before_id, 0))) as unread_count
                from chats c
                join chat_members me on me.chat_id = c.id and me.user_id = $1 and me.left_at_ms is null
                left join lateral (
                    select body, created_at_ms, media_kind, deleted_at_ms from messages m
                    where m.chat_id = c.id and m.id > coalesce(me.cleared_before_id, 0)
                    order by m.id desc limit 1
                ) lm on true
                where c.kind in ('group', 'channel')
             ) combined (chat_id, other_user_id, other_username, body, created_at_ms, media_kind, deleted_at_ms, is_self, is_archived, is_group, is_channel, title, description, avatar_media_id, member_count, handle, my_role, unread_count)
             order by coalesce(created_at_ms, 0) desc",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ChatSummary {
                chat_id: row.chat_id,
                other_user_id: row.other_user_id,
                other_username: row.other_username,
                last_message: row.created_at_ms.map(|created_at_ms| LastMessage {
                    text: row.body.unwrap_or_default(),
                    created_at_ms,
                    media_kind: row.media_kind,
                    deleted: row.deleted_at_ms.is_some(),
                }),
                is_self: row.is_self,
                is_archived: row.is_archived,
                is_group: row.is_group,
                is_channel: row.is_channel,
                title: row.title,
                description: row.description,
                avatar_media_id: row.avatar_media_id,
                member_count: row.member_count,
                handle: row.handle,
                my_role: row.my_role,
                unread_count: row.unread_count,
            })
            .collect())
    }

    /// Which chats this connection should subscribe to on WS connect — a
    /// left chat (see `leave_chat`) is excluded, so a former member simply
    /// stops receiving its fan-out, same as never having joined.
    pub async fn chats_for_user(&self, user_id: i64) -> Result<Vec<i64>, ChatError> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            "select chat_id from chat_members where user_id = $1 and left_at_ms is null",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Looks up a media attachment's recorded kind/mime/name/size by the
    /// (chat_id, media_id) pair a signed URL vouches for — the gateway's
    /// media-serving and -forwarding endpoints use this instead of trusting
    /// anything the request itself claims about the file.
    pub async fn media_lookup(
        &self,
        chat_id: i64,
        media_id: i64,
    ) -> Result<Option<(String, String, Option<String>, i64)>, ChatError> {
        let row: Option<(String, String, Option<String>, i64)> = sqlx::query_as(
            "select media_kind, media_mime, media_name, media_size from messages
             where chat_id = $1 and media_id = $2
             limit 1",
        )
        .bind(chat_id)
        .bind(media_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// A single primary-key lookup resolving `reply_to_id` into the snapshot
    /// a client needs to render a reply quote. Scoped to `chat_id` too, so a
    /// forged `reply_to_id` can't leak a preview of a message from a chat
    /// the caller isn't even in.
    pub async fn reply_preview(&self, chat_id: i64, message_id: i64) -> Result<Option<ReplyPreviewRef>, ChatError> {
        let row: Option<(i64, String, Option<String>, Option<i64>)> = sqlx::query_as(
            "select author_id, body, media_kind, deleted_at_ms from messages where id = $1 and chat_id = $2",
        )
        .bind(message_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(author_id, text, media_kind, deleted_at_ms)| ReplyPreviewRef {
            message_id,
            author_id,
            text: truncate_preview(&text),
            media_kind,
            deleted: deleted_at_ms.is_some(),
        }))
    }

    pub async fn insert_message(
        &self,
        chat_id: i64,
        author_id: i64,
        text: String,
        media: Option<MediaRef>,
        reply_to_id: Option<i64>,
        forwarded_from: Option<ForwardedFromRef>,
        kind: &str,
    ) -> Result<Message, ChatError> {
        let id = self.ids.next_id();
        let created_at_ms = now_ms();
        sqlx::query(
            "insert into messages
                (id, chat_id, author_id, body, created_at_ms,
                 media_id, media_kind, media_mime, media_name, media_size, media_duration_ms,
                 reply_to_id, forwarded_from_user_id, forwarded_from_username, kind)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        )
        .bind(id)
        .bind(chat_id)
        .bind(author_id)
        .bind(&text)
        .bind(created_at_ms)
        .bind(media.as_ref().map(|m| m.media_id))
        .bind(media.as_ref().map(|m| &m.kind))
        .bind(media.as_ref().map(|m| &m.mime))
        .bind(media.as_ref().and_then(|m| m.name.as_ref()))
        .bind(media.as_ref().map(|m| m.size))
        .bind(media.as_ref().and_then(|m| m.duration_ms))
        .bind(reply_to_id)
        .bind(forwarded_from.as_ref().map(|f| f.user_id))
        .bind(forwarded_from.as_ref().map(|f| &f.username))
        .bind(kind)
        .execute(&self.pool)
        .await?;

        let reply_preview = match reply_to_id {
            Some(reply_to_id) => self.reply_preview(chat_id, reply_to_id).await?,
            None => None,
        };

        Ok(Message {
            id,
            chat_id,
            author_id,
            text,
            created_at_ms,
            media,
            reply_to_id,
            reply_preview,
            forwarded_from,
            edited_at_ms: None,
            deleted_at_ms: None,
            kind: kind.to_string(),
            pinned_at_ms: None,
        })
    }

    /// A server-generated notice ("Alice created the group", "Alice added
    /// Bob and Carol") — persisted and broadcast exactly like an ordinary
    /// message, just with `kind = "system"` so clients render it as a
    /// centered pill instead of a normal bubble. `actor_id` is whoever
    /// performed the action (always a real member, never a synthetic
    /// system user) so existing author-keyed rendering keeps working.
    pub async fn insert_system_message(&self, chat_id: i64, actor_id: i64, body: String) -> Result<Message, ChatError> {
        self.insert_message(chat_id, actor_id, body, None, None, None, "system").await
    }

    /// Partitioned conceptually by `(chat_id, id)` — `id` is a time-sortable
    /// Snowflake, so this index gives cheap recent-first pagination without
    /// a separate `created_at` sort key. Moving to ScyllaDB later keeps the
    /// same partition/clustering shape.
    ///
    /// `before`, when set, returns the page older than that message ID —
    /// this is the infinite-scroll-upward cursor. `None` gets the most
    /// recent page.
    pub async fn recent_messages(
        &self,
        chat_id: i64,
        user_id: i64,
        limit: i64,
        before: Option<i64>,
    ) -> Result<Vec<Message>, ChatError> {
        // The left join resolves each row's reply preview inline — one
        // query, no N+1 — by matching on `r.id`, the other side's primary
        // key. Rows with no reply_to_id simply join nothing. Aliased (not a
        // plain tuple) because sqlx's tuple FromRow impl stops well short
        // of this many columns. The chat_members join brings in *this
        // caller's* cleared_before_id — clearing only ever hides history
        // from the clearer's own fetches, never the other member's.
        const FROM: &str = "from messages m
             left join messages r on r.id = m.reply_to_id
             join chat_members cm on cm.chat_id = m.chat_id and cm.user_id = $2";
        let rows: Vec<MessageRow> = match before {
            Some(before) => {
                sqlx::query_as(&format!(
                    "select {MESSAGE_COLUMNS} {FROM}
                     where m.chat_id = $1 and m.id > coalesce(cm.cleared_before_id, 0) and m.id < $3
                     order by m.id desc limit $4",
                ))
                .bind(chat_id)
                .bind(user_id)
                .bind(before)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(&format!(
                    "select {MESSAGE_COLUMNS} {FROM}
                     where m.chat_id = $1 and m.id > coalesce(cm.cleared_before_id, 0)
                     order by m.id desc limit $3",
                ))
                .bind(chat_id)
                .bind(user_id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows.into_iter().map(row_to_message).collect())
    }

    /// Pins (or re-pins) a message — chat-wide, visible to every member,
    /// not gated per-caller the way clearing/archiving are. `pinned_at_ms`
    /// also drives cycling order in `pinned_messages` (oldest pin first,
    /// same as Telegram's pinned-message bar). Not membership-gated itself;
    /// callers must `is_member`-gate this the same way `group_members` does.
    pub async fn pin_message(&self, chat_id: i64, message_id: i64) -> Result<(), ChatError> {
        sqlx::query(
            "update messages set pinned_at_ms = $3
             where id = $1 and chat_id = $2 and deleted_at_ms is null",
        )
        .bind(message_id)
        .bind(chat_id)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn unpin_message(&self, chat_id: i64, message_id: i64) -> Result<(), ChatError> {
        sqlx::query("update messages set pinned_at_ms = null where id = $1 and chat_id = $2")
            .bind(message_id)
            .bind(chat_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every currently pinned message in the chat, oldest pin first — the
    /// order the pinned-message bar cycles through on repeated clicks.
    pub async fn pinned_messages(&self, chat_id: i64) -> Result<Vec<Message>, ChatError> {
        let rows: Vec<MessageRow> = sqlx::query_as(&format!(
            "select {MESSAGE_COLUMNS}
             from messages m
             left join messages r on r.id = m.reply_to_id
             where m.chat_id = $1 and m.pinned_at_ms is not null and m.deleted_at_ms is null
             order by m.pinned_at_ms asc",
        ))
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(row_to_message).collect())
    }

    /// Hides every message currently in the chat from this caller's future
    /// fetches (via `recent_messages`'s cleared_before_id filter) without
    /// touching the other member's view or deleting anything. New messages
    /// sent afterward are unaffected.
    pub async fn clear_chat(&self, chat_id: i64, user_id: i64) -> Result<(), ChatError> {
        sqlx::query(
            "update chat_members
             set cleared_before_id = coalesce((select max(id) from messages where chat_id = $1), 0)
             where chat_id = $1 and user_id = $2",
        )
        .bind(chat_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Marks everything currently in the chat as read for this caller —
    /// sets `last_read_message_id` to the chat's current latest message id
    /// (never moves it backward, in case of a race with a message that
    /// arrived between the caller loading the chat and this call landing).
    /// Mirrors `clear_chat`'s exact shape, just for "I've seen this"
    /// (affects `unread_count` in `list_chats_for_user`) instead of "hide
    /// this from my view".
    pub async fn mark_chat_read(&self, chat_id: i64, user_id: i64) -> Result<(), ChatError> {
        sqlx::query(
            "update chat_members
             set last_read_message_id = greatest(
                 coalesce(last_read_message_id, 0),
                 coalesce((select max(id) from messages where chat_id = $1), 0)
             )
             where chat_id = $1 and user_id = $2",
        )
        .bind(chat_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Hides this chat from the caller's own main chat list (see
    /// `list_direct_chats_for_user`'s `is_archived`) without touching
    /// membership, messages, or the other member's view. Sending/receiving
    /// still works as normal — archiving is purely a list-visibility flag.
    pub async fn archive_chat(&self, chat_id: i64, user_id: i64) -> Result<(), ChatError> {
        sqlx::query("update chat_members set archived_at_ms = $3 where chat_id = $1 and user_id = $2")
            .bind(chat_id)
            .bind(user_id)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn unarchive_chat(&self, chat_id: i64, user_id: i64) -> Result<(), ChatError> {
        sqlx::query("update chat_members set archived_at_ms = null where chat_id = $1 and user_id = $2")
            .bind(chat_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// "Delete chat" for this caller only — they stop being an active
    /// member (see `is_member`/`chats_for_user`), so the chat drops out of
    /// their list and WS subscriptions, but the row and every message stay
    /// intact for the other member. Messaging the same person again later
    /// starts a brand new chat (see `find_direct_chat`).
    pub async fn leave_chat(&self, chat_id: i64, user_id: i64) -> Result<(), ChatError> {
        sqlx::query("update chat_members set left_at_ms = $3 where chat_id = $1 and user_id = $2")
            .bind(chat_id)
            .bind(user_id)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Edits a message's text in place. `None` means either the message
    /// doesn't exist, isn't in this chat, was already deleted, or isn't
    /// authored by `caller_id` — all collapsed into one outcome since the
    /// gateway responds to each the same way (a 403/not-applicable error),
    /// and distinguishing them would leak which messages exist in chats
    /// the caller can't actually see.
    pub async fn edit_message(
        &self,
        chat_id: i64,
        message_id: i64,
        caller_id: i64,
        text: String,
    ) -> Result<Option<(i64, String, i64)>, ChatError> {
        let edited_at_ms = now_ms();
        let row: Option<(i64,)> = sqlx::query_as(
            "update messages set body = $1, edited_at_ms = $2
             where id = $3 and chat_id = $4 and author_id = $5 and deleted_at_ms is null
             returning id",
        )
        .bind(&text)
        .bind(edited_at_ms)
        .bind(message_id)
        .bind(chat_id)
        .bind(caller_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id,)| (id, text, edited_at_ms)))
    }

    /// Soft-deletes a message "for everyone": the row stays (so ordering
    /// and reply quotes pointing at it keep working — they just resolve to
    /// a tombstone), but `body` and every media_* column are scrubbed. The
    /// media's pre-delete kind/id (if any) comes back so the caller can
    /// also remove the underlying file from disk — this method only
    /// touches Postgres, and a plain `UPDATE ... RETURNING` can't report
    /// pre-update values, hence the select-then-update.
    pub async fn delete_message(
        &self,
        chat_id: i64,
        message_id: i64,
        caller_id: i64,
    ) -> Result<Option<DeletedMessage>, ChatError> {
        type ExistingRow = (
            i64,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
        );
        let existing: Option<ExistingRow> = sqlx::query_as(
            "select author_id, media_id, media_kind, media_mime, media_name, media_size, media_duration_ms
             from messages
             where id = $1 and chat_id = $2 and deleted_at_ms is null",
        )
        .bind(message_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some((author_id, media_id, media_kind, media_mime, media_name, media_size, media_duration_ms)) = existing
        else {
            return Ok(None);
        };
        if author_id != caller_id {
            return Ok(None);
        }

        let deleted_at_ms = now_ms();
        sqlx::query(
            "update messages set
                body = '', media_id = null, media_kind = null, media_mime = null,
                media_name = null, media_size = null, media_duration_ms = null, deleted_at_ms = $1
             where id = $2",
        )
        .bind(deleted_at_ms)
        .bind(message_id)
        .execute(&self.pool)
        .await?;

        let old_media = media_id.map(|media_id| MediaRef {
            media_id,
            kind: media_kind.unwrap_or_default(),
            mime: media_mime.unwrap_or_default(),
            name: media_name,
            size: media_size.unwrap_or(0),
            duration_ms: media_duration_ms,
        });

        Ok(Some(DeletedMessage { deleted_at_ms, old_media }))
    }

    pub async fn block_user(&self, blocker_id: i64, blocked_id: i64) -> Result<(), ChatError> {
        sqlx::query(
            "insert into blocks (blocker_id, blocked_id, created_at_ms) values ($1, $2, $3)
             on conflict (blocker_id, blocked_id) do nothing",
        )
        .bind(blocker_id)
        .bind(blocked_id)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn unblock_user(&self, blocker_id: i64, blocked_id: i64) -> Result<(), ChatError> {
        sqlx::query("delete from blocks where blocker_id = $1 and blocked_id = $2")
            .bind(blocker_id)
            .bind(blocked_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// True if either side has blocked the other — sending/starting a chat
    /// is refused either way, matching the common "you can't message
    /// someone you've blocked without unblocking first" rule.
    pub async fn is_blocked(&self, user_a: i64, user_b: i64) -> Result<bool, ChatError> {
        let row: Option<(i32,)> = sqlx::query_as(
            "select 1 from blocks
             where (blocker_id = $1 and blocked_id = $2) or (blocker_id = $2 and blocked_id = $1)
             limit 1",
        )
        .bind(user_a)
        .bind(user_b)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub async fn list_blocked(&self, blocker_id: i64) -> Result<Vec<(i64, String)>, ChatError> {
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "select u.id, u.username from blocks b
             join users u on u.id = b.blocked_id
             where b.blocker_id = $1
             order by b.created_at_ms desc",
        )
        .bind(blocker_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Just records the report — callers compose this with `block_user`
    /// and/or `leave_chat` for the "block, report spam, and leave" action
    /// a client actually exposes; kept separate here since each is a
    /// distinct, independently useful primitive.
    pub async fn report_spam(&self, reporter_id: i64, reported_id: i64, chat_id: Option<i64>) -> Result<(), ChatError> {
        let id = self.ids.next_id();
        sqlx::query(
            "insert into spam_reports (id, reporter_id, reported_id, chat_id, created_at_ms)
             values ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(reporter_id)
        .bind(reported_id)
        .bind(chat_id)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeletedMessage {
    pub deleted_at_ms: i64,
    pub old_media: Option<MediaRef>,
}

// Shared by `recent_messages` and `pinned_messages` — aliased (not a plain
// tuple) because sqlx's tuple FromRow impl stops well short of this many
// columns. The reply preview is resolved inline via the self-join on `r`,
// matching on `r.id` — one query, no N+1; rows with no reply_to_id simply
// join nothing.
const MESSAGE_COLUMNS: &str = "m.id, m.chat_id, m.author_id, m.body, m.created_at_ms,
     m.media_id, m.media_kind, m.media_mime, m.media_name, m.media_size, m.media_duration_ms,
     m.reply_to_id, m.forwarded_from_user_id, m.forwarded_from_username,
     m.edited_at_ms, m.deleted_at_ms, m.kind, m.pinned_at_ms,
     r.author_id as reply_author_id, r.body as reply_text, r.media_kind as reply_media_kind,
     r.deleted_at_ms as reply_deleted_at_ms";

#[derive(sqlx::FromRow)]
struct MessageRow {
    id: i64,
    chat_id: i64,
    author_id: i64,
    body: String,
    created_at_ms: i64,
    media_id: Option<i64>,
    media_kind: Option<String>,
    media_mime: Option<String>,
    media_name: Option<String>,
    media_size: Option<i64>,
    media_duration_ms: Option<i64>,
    reply_to_id: Option<i64>,
    forwarded_from_user_id: Option<i64>,
    forwarded_from_username: Option<String>,
    edited_at_ms: Option<i64>,
    deleted_at_ms: Option<i64>,
    kind: String,
    pinned_at_ms: Option<i64>,
    reply_author_id: Option<i64>,
    reply_text: Option<String>,
    reply_media_kind: Option<String>,
    reply_deleted_at_ms: Option<i64>,
}

fn row_to_message(row: MessageRow) -> Message {
    let media = row.media_id.map(|media_id| MediaRef {
        media_id,
        kind: row.media_kind.unwrap_or_default(),
        mime: row.media_mime.unwrap_or_default(),
        name: row.media_name,
        size: row.media_size.unwrap_or(0),
        duration_ms: row.media_duration_ms,
    });
    let reply_preview = row.reply_to_id.zip(row.reply_author_id).map(|(message_id, author_id)| ReplyPreviewRef {
        message_id,
        author_id,
        text: truncate_preview(&row.reply_text.unwrap_or_default()),
        media_kind: row.reply_media_kind,
        deleted: row.reply_deleted_at_ms.is_some(),
    });
    let forwarded_from = row
        .forwarded_from_user_id
        .zip(row.forwarded_from_username)
        .map(|(user_id, username)| ForwardedFromRef { user_id, username });
    Message {
        id: row.id,
        chat_id: row.chat_id,
        author_id: row.author_id,
        text: row.body,
        created_at_ms: row.created_at_ms,
        media,
        reply_to_id: row.reply_to_id,
        reply_preview,
        forwarded_from,
        edited_at_ms: row.edited_at_ms,
        deleted_at_ms: row.deleted_at_ms,
        kind: row.kind,
        pinned_at_ms: row.pinned_at_ms,
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

// Same rule as `chat_auth`'s identical (private, unexported) validator for
// `users.handle` — kept in sync by hand rather than sharing a crate, since
// it's a 7-line rule and a cross-crate dependency just for this would be
// more ceremony than the duplication it avoids. 3-32 chars, lowercase ascii
// letters/digits/underscore, must start with a letter.
fn is_valid_handle(handle: &str) -> bool {
    let mut chars = handle.chars();
    let Some(first) = chars.next() else { return false };
    if !first.is_ascii_lowercase() {
        return false;
    }
    (3..=32).contains(&handle.len())
        && handle.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(
        err.as_database_error().and_then(|db_err| db_err.code()),
        Some(code) if code == "23505"
    )
}
