-- A free-text profile bio (description, portfolio/social links, etc.) —
-- same ~500-char cap enforced client-side and re-checked at the HTTP
-- layer, not a DB constraint, matching how every other length limit in
-- this app (group description, message text) is handled.
alter table users
    add column bio text;
