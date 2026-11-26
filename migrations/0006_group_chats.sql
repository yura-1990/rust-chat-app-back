-- Group metadata lives directly on `chats` (nullable, group-only — mirrors
-- how `chats.kind` is already a free-text column with no CHECK constraint).
alter table chats
    add column title text,
    add column description text,
    add column avatar_media_id bigint,
    add column created_by bigint references users(id);

-- 'owner' | 'member' — only the owner can manage the group for now.
alter table chat_members
    add column role text not null default 'member';

-- Lets a message be a server-generated system notice ("Alice added Bob")
-- instead of authored chat text, rendered as a centered pill client-side.
-- These are real persisted/broadcast messages, so every member's history
-- has them, unlike the client-only call-event rows.
alter table messages
    add column kind text not null default 'text';
