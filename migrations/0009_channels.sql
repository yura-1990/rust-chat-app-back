-- Channels are `chats.kind = 'channel'` (free-text column, no CHECK —
-- same precedent as 'group'). A channel's public handle is optional: null
-- means invite-only, matching how a user without `users.handle` set isn't
-- publicly searchable either.
alter table chats
    add column handle text unique;

-- chat_members.role gains a third value, 'admin', alongside the existing
-- 'owner'/'member' — still just a free-text column, no migration needed
-- for the value itself, documented here for anyone reading the schema.
comment on column chat_members.role is 'owner | admin | member';
