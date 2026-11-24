-- Media attachments (photos, files, round video messages). A message can
-- carry text, media, or both (media + caption). The actual bytes live on
-- local disk under .data/media/<chat_id>/<media_id>.<ext> — this table only
-- tracks metadata; access is via short-lived HMAC-signed URLs the gateway
-- mints on demand (see crates/gateway/src/media.rs), never a stored URL.
alter table messages
    add column media_id bigint,
    add column media_kind text,
    add column media_mime text,
    add column media_name text,
    add column media_size bigint,
    add column media_duration_ms bigint;
