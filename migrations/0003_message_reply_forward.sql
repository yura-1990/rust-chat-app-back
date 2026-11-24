alter table messages
    add column reply_to_id bigint,
    add column forwarded_from_user_id bigint,
    add column forwarded_from_username text;
