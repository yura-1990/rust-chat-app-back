-- Per-member archive state, same pattern as `cleared_before_id`: archiving
-- only hides the chat from the archiver's own list, never the other
-- member's, and doesn't affect message rows or WS subscriptions.
alter table chat_members
    add column archived_at_ms bigint;
