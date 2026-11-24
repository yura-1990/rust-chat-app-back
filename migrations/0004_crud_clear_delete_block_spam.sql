-- Message edit/delete: delete is a soft tombstone (body/media cleared,
-- deleted_at_ms set) so every member's history still shows "message
-- deleted" in place rather than a gap; edit just tracks when, the new body
-- overwrites the old one in place (no edit history kept).
alter table messages
    add column edited_at_ms bigint,
    add column deleted_at_ms bigint;

-- Per-member chat state: clearing only affects the clearer's own view
-- (recent_messages filters id > cleared_before_id for that caller); leaving
-- stops the chat showing in that member's list / WS subscriptions without
-- touching the other member's copy or any message rows.
alter table chat_members
    add column cleared_before_id bigint,
    add column left_at_ms bigint;

create table blocks (
    blocker_id bigint not null references users(id) on delete cascade,
    blocked_id bigint not null references users(id) on delete cascade,
    created_at_ms bigint not null,
    primary key (blocker_id, blocked_id)
);

create table spam_reports (
    id bigint primary key,
    reporter_id bigint not null references users(id) on delete cascade,
    reported_id bigint not null references users(id) on delete cascade,
    chat_id bigint references chats(id) on delete set null,
    created_at_ms bigint not null
);
