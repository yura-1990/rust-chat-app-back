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
pub struct DirectChatSummary {
    pub chat_id: i64,
    pub other_user_id: i64,
    pub other_username: String,
    pub last_message: Option<LastMessage>,
    /// "Saved Messages" — a chat with no second member, used as a personal
    /// notes-to-self space (and, since it fans out over the same WS
    /// subscription as any other chat, syncs across every session the
    /// user has open). `other_user_id`/`other_username` are the caller's
    /// own identity in this case — there's no one else in the chat.
    pub is_self: bool,
    pub is_archived: bool,
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

    /// Every direct chat the user belongs to, newest-activity first, with
    /// the other participant's identity and last message preview — exactly
    /// what a chat-list sidebar needs in one round trip.
    pub async fn list_direct_chats_for_user(&self, user_id: i64) -> Result<Vec<DirectChatSummary>, ChatError> {
        type Row = (
            i64,
            i64,
            String,
            Option<String>,
            Option<i64>,
            Option<String>,
            Option<i64>,
            bool,
            bool,
        );
        // Unioned rather than one query with an outer-joined "other" member,
        // because a direct chat's "other" is a different row (other.user_id
        // <> $1) while a self chat's "other" is the same row as "me" —
        // genuinely two different shapes of query, not one query with an
        // optional condition.
        // Postgres doesn't allow an expression (only a bare column name) in
        // a UNION's own ORDER BY, so the union is wrapped in a subquery and
        // ordered from the outside instead, where it's just a plain SELECT.
        let rows: Vec<Row> = sqlx::query_as(
            "select * from (
                select c.id, u.id, u.username, lm.body, lm.created_at_ms, lm.media_kind, lm.deleted_at_ms, false as is_self, me.archived_at_ms is not null as is_archived
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

                select c.id, u.id, u.username, lm.body, lm.created_at_ms, lm.media_kind, lm.deleted_at_ms, true as is_self, me.archived_at_ms is not null as is_archived
                from chats c
                join chat_members me on me.chat_id = c.id and me.user_id = $1 and me.left_at_ms is null
                join users u on u.id = me.user_id
                left join lateral (
                    select body, created_at_ms, media_kind, deleted_at_ms from messages m
                    where m.chat_id = c.id and m.id > coalesce(me.cleared_before_id, 0)
                    order by m.id desc limit 1
                ) lm on true
                where c.kind = 'self'
             ) combined (chat_id, other_user_id, other_username, body, created_at_ms, media_kind, deleted_at_ms, is_self, is_archived)
             order by coalesce(created_at_ms, 0) desc",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(chat_id, other_user_id, other_username, text, created_at_ms, media_kind, deleted_at_ms, is_self, is_archived)| DirectChatSummary {
                    chat_id,
                    other_user_id,
                    other_username,
                    last_message: created_at_ms.map(|created_at_ms| LastMessage {
                        text: text.unwrap_or_default(),
                        created_at_ms,
                        media_kind,
                        deleted: deleted_at_ms.is_some(),
                    }),
                    is_self,
                    is_archived,
                },
            )
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
    ) -> Result<Message, ChatError> {
        let id = self.ids.next_id();
        let created_at_ms = now_ms();
        sqlx::query(
            "insert into messages
                (id, chat_id, author_id, body, created_at_ms,
                 media_id, media_kind, media_mime, media_name, media_size, media_duration_ms,
                 reply_to_id, forwarded_from_user_id, forwarded_from_username)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
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
        })
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
        const COLUMNS: &str = "m.id, m.chat_id, m.author_id, m.body, m.created_at_ms,
             m.media_id, m.media_kind, m.media_mime, m.media_name, m.media_size, m.media_duration_ms,
             m.reply_to_id, m.forwarded_from_user_id, m.forwarded_from_username,
             m.edited_at_ms, m.deleted_at_ms,
             r.author_id as reply_author_id, r.body as reply_text, r.media_kind as reply_media_kind,
             r.deleted_at_ms as reply_deleted_at_ms";
        const FROM: &str = "from messages m
             left join messages r on r.id = m.reply_to_id
             join chat_members cm on cm.chat_id = m.chat_id and cm.user_id = $2";
        let rows: Vec<MessageRow> = match before {
            Some(before) => {
                sqlx::query_as(&format!(
                    "select {COLUMNS} {FROM}
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
                    "select {COLUMNS} {FROM}
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
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
