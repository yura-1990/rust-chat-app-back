create table users (
    id bigint primary key,
    username text not null unique,
    password_hash text not null,
    created_at timestamptz not null default now()
);

create table chats (
    id bigint primary key,
    kind text not null,
    created_at timestamptz not null default now()
);

create table chat_members (
    chat_id bigint not null references chats(id) on delete cascade,
    user_id bigint not null references users(id) on delete cascade,
    joined_at timestamptz not null default now(),
    primary key (chat_id, user_id)
);

create table messages (
    id bigint primary key,
    chat_id bigint not null references chats(id) on delete cascade,
    author_id bigint not null references users(id) on delete cascade,
    body text not null,
    created_at_ms bigint not null
);

create index messages_chat_id_id_idx on messages (chat_id, id desc);
