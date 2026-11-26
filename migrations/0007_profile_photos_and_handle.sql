-- A public, shareable "@handle" — separate from the private login
-- `username`. Nullable (not every user sets one); uniqueness is enforced
-- here at the DB level (Postgres allows any number of NULLs under a
-- unique constraint, so users without one don't collide).
alter table users
    add column handle text unique,
    add column active_avatar_media_id bigint;

-- A user's profile-photo gallery — many photos per user, any one of which
-- can be the "active" one (users.active_avatar_media_id). Deleting a row
-- here never implicitly changes which media_id is active unless it's the
-- one being deleted (handled in application code, not a DB trigger).
create table user_avatars (
    id bigint primary key,
    user_id bigint not null references users(id) on delete cascade,
    media_id bigint not null,
    created_at_ms bigint not null
);

create index user_avatars_user_id_idx on user_avatars (user_id, id desc);
