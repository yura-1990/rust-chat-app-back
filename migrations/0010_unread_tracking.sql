-- Per-member "how far have they read" marker, mirroring cleared_before_id's
-- shape — lets unread counts be computed server-side (via list_chats) and
-- survive a member being offline when messages/posts arrive, instead of
-- only existing as a client-local counter that resets to 0 on every fresh
-- page load or reconnect.
alter table chat_members
    add column last_read_message_id bigint;
