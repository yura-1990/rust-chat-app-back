use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::sync::{mpsc, Mutex};

use chat_proto::chat::v1::envelope::Payload;
use chat_proto::chat::v1::{
    CallAnswer, CallEnd, CallInvite, CallSignal, DeleteMessage, EditMessage, Envelope, Error as ProtoError,
    ForwardedFrom as ProtoForwardedFrom, HeartbeatAck, HelloAck, Media as ProtoMedia, MessageCreated, MessageDeleted,
    MessageEdited, Op, ReplyPreview as ProtoReplyPreview, SendMessage, Typing,
};
use chat_service::{ForwardedFromRef, MediaRef};

use crate::state::AppState;

pub async fn upgrade(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (sink, mut stream) = socket.split();

    let Some(user_id) = authenticate(&mut stream, &state).await else {
        return;
    };
    tracing::info!(user_id, "client authenticated");

    // Every outgoing frame — the hello ack, heartbeat acks, errors, and
    // fanned-out messages from NATS — funnels through this channel into a
    // single task that owns the socket's write half.
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
    let writer = tokio::spawn(async move {
        let mut sink = sink;
        while let Some(bytes) = rx.recv().await {
            if sink.send(Message::Binary(bytes.into())).await.is_err() {
                break;
            }
        }
    });

    if send_direct(&tx, &hello_ack_envelope()).await.is_err() {
        writer.abort();
        return;
    }

    let subscriptions = subscribe_to_memberships(&state, user_id, tx.clone()).await;

    while let Some(Ok(msg)) = stream.next().await {
        let data = match msg {
            Message::Binary(data) => data,
            Message::Close(_) => break,
            _ => continue,
        };

        let Ok(envelope) = Envelope::decode(data.as_ref()) else {
            let _ = send_direct(&tx, &error_envelope(400, "could not decode envelope")).await;
            continue;
        };

        match envelope.payload {
            Some(Payload::Heartbeat(_)) => {
                let ack = Envelope {
                    op: Op::HeartbeatAck as i32,
                    seq: envelope.seq,
                    payload: Some(Payload::HeartbeatAck(HeartbeatAck {
                        server_time_ms: now_ms(),
                    })),
                };
                if send_direct(&tx, &ack).await.is_err() {
                    break;
                }
            }
            Some(Payload::SendMessage(send_message)) => {
                if let Err(err) = handle_send_message(&state, user_id, send_message).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            Some(Payload::EditMessage(edit_message)) => {
                if let Err(err) = handle_edit_message(&state, user_id, edit_message).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            Some(Payload::DeleteMessage(delete_message)) => {
                if let Err(err) = handle_delete_message(&state, user_id, delete_message).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            Some(Payload::Typing(typing)) => {
                let chat_id = typing.chat_id as i64;
                let envelope = Envelope {
                    op: Op::Typing as i32,
                    seq: 0,
                    payload: Some(Payload::Typing(Typing {
                        user_id: user_id as u64,
                        ..typing
                    })),
                };
                if let Err(err) = relay_to_chat(&state, user_id, chat_id, envelope).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            // Calling is signaled entirely by relaying these four through
            // the same per-chat NATS subject messages use — the server
            // never inspects SDP/ICE contents, only checks membership and
            // stamps the real sender onto each one.
            Some(Payload::CallInvite(invite)) => {
                let chat_id = invite.chat_id as i64;
                let envelope = Envelope {
                    op: Op::CallInvite as i32,
                    seq: 0,
                    payload: Some(Payload::CallInvite(CallInvite {
                        user_id: user_id as u64,
                        ..invite
                    })),
                };
                if let Err(err) = relay_to_chat(&state, user_id, chat_id, envelope).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            Some(Payload::CallAnswer(answer)) => {
                let chat_id = answer.chat_id as i64;
                let envelope = Envelope {
                    op: Op::CallAnswer as i32,
                    seq: 0,
                    payload: Some(Payload::CallAnswer(CallAnswer {
                        user_id: user_id as u64,
                        ..answer
                    })),
                };
                if let Err(err) = relay_to_chat(&state, user_id, chat_id, envelope).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            Some(Payload::CallSignal(signal)) => {
                let chat_id = signal.chat_id as i64;
                let envelope = Envelope {
                    op: Op::CallSignal as i32,
                    seq: 0,
                    payload: Some(Payload::CallSignal(CallSignal {
                        user_id: user_id as u64,
                        ..signal
                    })),
                };
                if let Err(err) = relay_to_chat(&state, user_id, chat_id, envelope).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            Some(Payload::CallEnd(end)) => {
                let chat_id = end.chat_id as i64;
                let envelope = Envelope {
                    op: Op::CallEnd as i32,
                    seq: 0,
                    payload: Some(Payload::CallEnd(CallEnd {
                        user_id: user_id as u64,
                        ..end
                    })),
                };
                if let Err(err) = relay_to_chat(&state, user_id, chat_id, envelope).await {
                    let _ = send_direct(&tx, &err).await;
                }
            }
            _ => {
                let _ = send_direct(&tx, &error_envelope(400, "unexpected payload for this connection state")).await;
            }
        }
    }

    for handle in subscriptions.handles.lock().await.drain(..) {
        handle.abort();
    }
    writer.abort();
}

/// Reads the first frame, requires it to be `Hello`, and resolves its token
/// to a real user ID. Anything else (wrong op, bad token, timeout) closes
/// the connection without a reply — there's no session yet to reply on.
async fn authenticate(stream: &mut SplitStream<WebSocket>, state: &AppState) -> Option<i64> {
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next()).await;
    let Ok(Some(Ok(Message::Binary(data)))) = first else {
        return None;
    };
    let hello = match Envelope::decode(data.as_ref()).ok()?.payload {
        Some(Payload::Hello(hello)) => hello,
        _ => return None,
    };
    state.auth.verify_token(&hello.token).ok().map(|claims| claims.user_id)
}

/// All NATS subscriptions a connection is holding — both the per-chat ones
/// taken at `Hello` and any added afterward. Shared with the control-subject
/// listener so it can add a new chat subscription without the connection
/// reconnecting, and with the disconnect path so everything gets aborted.
struct Subscriptions {
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    chat_ids: Mutex<HashSet<i64>>,
}

async fn subscribe_to_memberships(state: &Arc<AppState>, user_id: i64, tx: mpsc::Sender<Vec<u8>>) -> Arc<Subscriptions> {
    let subs = Arc::new(Subscriptions {
        handles: Mutex::new(Vec::new()),
        chat_ids: Mutex::new(HashSet::new()),
    });

    let chat_ids = match state.chat.chats_for_user(user_id).await {
        Ok(ids) => ids,
        Err(err) => {
            tracing::warn!(?err, user_id, "failed to load chat memberships");
            Vec::new()
        }
    };
    for chat_id in chat_ids {
        subscribe_to_chat(state, chat_id, tx.clone(), &subs).await;
    }

    // Memberships are only snapshotted above — a chat created *after* this
    // connection opened (e.g. the other side was already online when you
    // started a new chat with them) would otherwise never reach them until
    // they reconnect. `create_direct_chat` publishes here to close that gap.
    let control_subject = format!("user.{user_id}.new_chat");
    match state.nats.subscribe(control_subject.clone()).await {
        Ok(mut subscriber) => {
            let state = state.clone();
            let tx = tx.clone();
            let subs_for_control = subs.clone();
            let handle = tokio::spawn(async move {
                while let Some(message) = subscriber.next().await {
                    let Ok(text) = std::str::from_utf8(&message.payload) else { continue };
                    let Ok(chat_id) = text.parse::<i64>() else { continue };
                    subscribe_to_chat(&state, chat_id, tx.clone(), &subs_for_control).await;
                }
            });
            subs.handles.lock().await.push(handle);
        }
        Err(err) => tracing::warn!(?err, %control_subject, "failed to subscribe to new-chat notifications"),
    }

    subs
}

/// Subscribes to a single chat's NATS subject and forwards every message
/// straight into this connection's outgoing channel. No-op if already
/// subscribed, so it's safe to call both at `Hello` and again from the
/// `new_chat` control listener for the same chat.
async fn subscribe_to_chat(state: &Arc<AppState>, chat_id: i64, tx: mpsc::Sender<Vec<u8>>, subs: &Arc<Subscriptions>) {
    {
        let mut chat_ids = subs.chat_ids.lock().await;
        if !chat_ids.insert(chat_id) {
            return;
        }
    }

    let subject = format!("chat.{chat_id}");
    match state.nats.subscribe(subject.clone()).await {
        Ok(mut subscriber) => {
            let handle = tokio::spawn(async move {
                while let Some(message) = subscriber.next().await {
                    if tx.send(message.payload.to_vec()).await.is_err() {
                        break;
                    }
                }
            });
            subs.handles.lock().await.push(handle);
        }
        Err(err) => tracing::warn!(?err, %subject, "failed to subscribe to chat"),
    }
}

/// Persists the message, then publishes it to NATS instead of replying on
/// this socket directly — the sender gets their own copy back through the
/// same subscription every other member uses, so every client (including
/// other tabs of the same user) sees one consistent delivery path.
async fn handle_send_message(state: &AppState, user_id: i64, send_message: SendMessage) -> Result<(), Envelope> {
    let chat_id = send_message.chat_id as i64;

    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(error_envelope(403, "not a member of this chat")),
        Err(err) => {
            tracing::error!(?err, "membership check failed");
            return Err(error_envelope(500, "database error"));
        }
    }

    reject_if_blocked(state, chat_id, user_id).await?;

    let incoming_media = send_message.media.as_ref().map(|m| MediaRef {
        media_id: m.media_id as i64,
        kind: m.kind.clone(),
        mime: m.mime.clone(),
        name: (!m.name.is_empty()).then(|| m.name.clone()),
        size: m.size as i64,
        duration_ms: (m.duration_ms != 0).then_some(m.duration_ms as i64),
    });
    let reply_to_id = (send_message.reply_to_id != 0).then_some(send_message.reply_to_id as i64);
    let forwarded_from = send_message.forwarded_from.as_ref().map(|f| ForwardedFromRef {
        user_id: f.user_id as i64,
        username: f.username.clone(),
    });

    let message = state
        .chat
        .insert_message(chat_id, user_id, send_message.text, incoming_media, reply_to_id, forwarded_from)
        .await
        .map_err(|err| {
            tracing::error!(?err, "failed to persist message");
            error_envelope(500, "failed to persist message")
        })?;

    // The URL is minted fresh here, never stored — the DB only ever holds
    // `media_id`, so this is the one place a (short-lived) link to the
    // actual bytes comes into existence.
    let media = message.media.as_ref().map(|m| ProtoMedia {
        media_id: m.media_id as u64,
        kind: m.kind.clone(),
        mime: m.mime.clone(),
        name: m.name.clone().unwrap_or_default(),
        size: m.size as u64,
        duration_ms: m.duration_ms.unwrap_or(0) as u64,
        url: state.media.signed_url(chat_id, m.media_id),
    });
    let reply_preview = message.reply_preview.as_ref().map(|r| ProtoReplyPreview {
        message_id: r.message_id as u64,
        author_id: r.author_id as u64,
        text: r.text.clone(),
        media_kind: r.media_kind.clone().unwrap_or_default(),
        deleted: r.deleted,
    });
    let forwarded_from = message.forwarded_from.as_ref().map(|f| ProtoForwardedFrom {
        user_id: f.user_id as u64,
        username: f.username.clone(),
    });

    let envelope = Envelope {
        op: Op::MessageCreated as i32,
        seq: 0,
        payload: Some(Payload::MessageCreated(MessageCreated {
            id: message.id as u64,
            chat_id: message.chat_id as u64,
            author_id: message.author_id as u64,
            text: message.text,
            created_at_ms: message.created_at_ms,
            client_nonce: send_message.client_nonce,
            media,
            reply_to_id: message.reply_to_id.unwrap_or(0) as u64,
            reply_preview,
            forwarded_from,
        })),
    };

    publish_to_chat(state, chat_id, &envelope).await.map_err(|err| {
        tracing::error!(?err, "failed to publish message to nats");
        error_envelope(502, "message saved but could not be delivered")
    })
}

/// A direct chat has exactly one "other" member — if either side has
/// blocked the other, sending into this chat is refused, same rule as
/// starting one in the first place (see `http::create_direct_chat`).
async fn reject_if_blocked(state: &AppState, chat_id: i64, user_id: i64) -> Result<(), Envelope> {
    let members = state.chat.member_ids(chat_id).await.map_err(|err| {
        tracing::error!(?err, "member lookup failed");
        error_envelope(500, "database error")
    })?;
    for other in members.into_iter().filter(|&id| id != user_id) {
        match state.chat.is_blocked(user_id, other).await {
            Ok(true) => return Err(error_envelope(403, "blocked")),
            Ok(false) => {}
            Err(err) => {
                tracing::error!(?err, "block check failed");
                return Err(error_envelope(500, "database error"));
            }
        }
    }
    Ok(())
}

/// Edits are author-only and only ever change `text` — media/reply/forward
/// references on the message are immutable once sent.
async fn handle_edit_message(state: &AppState, user_id: i64, edit_message: EditMessage) -> Result<(), Envelope> {
    let chat_id = edit_message.chat_id as i64;

    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(error_envelope(403, "not a member of this chat")),
        Err(err) => {
            tracing::error!(?err, "membership check failed");
            return Err(error_envelope(500, "database error"));
        }
    }

    let edited = state
        .chat
        .edit_message(chat_id, edit_message.message_id as i64, user_id, edit_message.text)
        .await
        .map_err(|err| {
            tracing::error!(?err, "failed to edit message");
            error_envelope(500, "failed to edit message")
        })?;
    let Some((id, text, edited_at_ms)) = edited else {
        return Err(error_envelope(404, "message not found, already deleted, or not yours"));
    };

    let envelope = Envelope {
        op: Op::MessageEdited as i32,
        seq: 0,
        payload: Some(Payload::MessageEdited(MessageEdited {
            id: id as u64,
            chat_id: chat_id as u64,
            text,
            edited_at_ms,
        })),
    };
    publish_to_chat(state, chat_id, &envelope).await.map_err(|err| {
        tracing::error!(?err, "failed to publish edit to nats");
        error_envelope(502, "edit saved but could not be delivered")
    })
}

/// Deletes are author-only, "for everyone" (there's no delete-for-me-only
/// on messages — that's what `clear_chat` is for), and soft: the row and
/// its id stay so reply quotes pointing at it keep resolving to a
/// tombstone instead of breaking.
async fn handle_delete_message(state: &AppState, user_id: i64, delete_message: DeleteMessage) -> Result<(), Envelope> {
    let chat_id = delete_message.chat_id as i64;
    let message_id = delete_message.message_id as i64;

    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(error_envelope(403, "not a member of this chat")),
        Err(err) => {
            tracing::error!(?err, "membership check failed");
            return Err(error_envelope(500, "database error"));
        }
    }

    let deleted = state.chat.delete_message(chat_id, message_id, user_id).await.map_err(|err| {
        tracing::error!(?err, "failed to delete message");
        error_envelope(500, "failed to delete message")
    })?;
    let Some(deleted) = deleted else {
        return Err(error_envelope(404, "message not found, already deleted, or not yours"));
    };

    if let Some(old_media) = deleted.old_media {
        state.media.delete(chat_id, old_media.media_id, &old_media.kind, &old_media.mime).await;
    }

    let envelope = Envelope {
        op: Op::MessageDeleted as i32,
        seq: 0,
        payload: Some(Payload::MessageDeleted(MessageDeleted {
            id: message_id as u64,
            chat_id: chat_id as u64,
            deleted_at_ms: deleted.deleted_at_ms,
        })),
    };
    publish_to_chat(state, chat_id, &envelope).await.map_err(|err| {
        tracing::error!(?err, "failed to publish delete to nats");
        error_envelope(502, "delete saved but could not be delivered")
    })
}

/// Shared by everything that's pure fan-out with no persistence (typing,
/// call signaling): checks membership, then republishes the envelope
/// verbatim to the chat's NATS subject. Never touches Postgres beyond the
/// membership check.
async fn relay_to_chat(state: &AppState, user_id: i64, chat_id: i64, envelope: Envelope) -> Result<(), Envelope> {
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return Err(error_envelope(403, "not a member of this chat")),
        Err(err) => {
            tracing::error!(?err, "membership check failed");
            return Err(error_envelope(500, "database error"));
        }
    }

    publish_to_chat(state, chat_id, &envelope).await.map_err(|err| {
        tracing::error!(?err, "failed to relay envelope to nats");
        error_envelope(502, "could not deliver")
    })
}

async fn publish_to_chat(state: &AppState, chat_id: i64, envelope: &Envelope) -> Result<(), async_nats::PublishError> {
    state.nats.publish(format!("chat.{chat_id}"), encode(envelope).into()).await
}

fn hello_ack_envelope() -> Envelope {
    Envelope {
        op: Op::HelloAck as i32,
        seq: 0,
        payload: Some(Payload::HelloAck(HelloAck {
            session_id: uuid::Uuid::new_v4().to_string(),
            server_time_ms: now_ms(),
        })),
    }
}

fn error_envelope(code: u32, message: &str) -> Envelope {
    Envelope {
        op: Op::Error as i32,
        seq: 0,
        payload: Some(Payload::Error(ProtoError {
            code,
            message: message.to_string(),
        })),
    }
}

fn encode(envelope: &Envelope) -> Vec<u8> {
    let mut buf = Vec::with_capacity(envelope.encoded_len());
    envelope
        .encode(&mut buf)
        .expect("encoding a valid message into a growable buffer cannot fail");
    buf
}

async fn send_direct(tx: &mpsc::Sender<Vec<u8>>, envelope: &Envelope) -> Result<(), ()> {
    tx.send(encode(envelope)).await.map_err(|_| ())
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
