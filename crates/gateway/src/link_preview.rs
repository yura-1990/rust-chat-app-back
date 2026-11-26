use std::net::{IpAddr, ToSocketAddrs};
use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;
use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct LinkPreview {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub site_name: Option<String>,
}

const MAX_BODY_BYTES: u64 = 512 * 1024;

fn ip_is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_documentation())
        }
        IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80) // link-local fe80::/10
        }
    }
}

/// Resolves `host` and rejects anything that isn't a public address — the
/// SSRF guard for both the initial request and every redirect hop, since a
/// malicious/compromised target could otherwise point this server-side
/// fetch at the gateway's own internal network (localhost, link-local
/// metadata IPs, RFC1918 ranges, etc).
fn host_is_safe(host: &str) -> bool {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return ip_is_public(ip);
    }
    let addrs: Vec<IpAddr> = match (host, 0u16).to_socket_addrs() {
        Ok(iter) => iter.map(|sa| sa.ip()).collect(),
        Err(_) => return false,
    };
    !addrs.is_empty() && addrs.iter().all(|ip| ip_is_public(*ip))
}

pub fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .connect_timeout(Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 {
                return attempt.error("too many redirects");
            }
            match attempt.url().host_str() {
                Some(host) if host_is_safe(host) => attempt.follow(),
                _ => attempt.stop(),
            }
        }))
        .user_agent("Mozilla/5.0 (compatible; ChatLinkPreview/1.0)")
        .build()
        .expect("failed to build link-preview http client")
}

static META_RE: OnceLock<Regex> = OnceLock::new();
static TITLE_RE: OnceLock<Regex> = OnceLock::new();

fn meta_re() -> &'static Regex {
    META_RE.get_or_init(|| {
        Regex::new(
            r#"(?is)<meta\s+[^>]*?(?:property|name)\s*=\s*["']([^"']+)["'][^>]*?content\s*=\s*["']([^"']*)["'][^>]*>|<meta\s+[^>]*?content\s*=\s*["']([^"']*)["'][^>]*?(?:property|name)\s*=\s*["']([^"']+)["'][^>]*>"#,
        )
        .expect("static regex")
    })
}

fn title_re() -> &'static Regex {
    TITLE_RE.get_or_init(|| Regex::new(r#"(?is)<title[^>]*>(.*?)</title>"#).expect("static regex"))
}

fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn parse_html(html: &str, url: &str) -> LinkPreview {
    let mut title = None;
    let mut description = None;
    let mut image = None;
    let mut site_name = None;

    for caps in meta_re().captures_iter(html) {
        let (key, value) = match (caps.get(1), caps.get(4)) {
            (Some(k), _) => (k.as_str(), caps.get(2).map(|m| m.as_str()).unwrap_or("")),
            (None, Some(k)) => (k.as_str(), caps.get(3).map(|m| m.as_str()).unwrap_or("")),
            _ => continue,
        };
        let value = decode_entities(value.trim());
        if value.is_empty() {
            continue;
        }
        match key.to_ascii_lowercase().as_str() {
            "og:title" | "twitter:title" if title.is_none() => title = Some(value),
            "og:description" | "twitter:description" | "description" if description.is_none() => {
                description = Some(value)
            }
            "og:image" | "og:image:url" | "twitter:image" if image.is_none() => image = Some(value),
            "og:site_name" if site_name.is_none() => site_name = Some(value),
            _ => {}
        }
    }

    if title.is_none() {
        if let Some(caps) = title_re().captures(html) {
            let t = decode_entities(caps[1].trim());
            if !t.is_empty() {
                title = Some(t);
            }
        }
    }

    if let Some(raw_image) = image.clone() {
        if let Ok(base) = reqwest::Url::parse(url) {
            if let Ok(resolved) = base.join(&raw_image) {
                image = Some(resolved.to_string());
            }
        }
    }

    LinkPreview {
        url: url.to_string(),
        title,
        description,
        image,
        site_name,
    }
}

/// Fetches `url` server-side and scrapes Open Graph / Twitter Card /
/// `<title>` metadata for a Telegram-style link preview. Returns `None` on
/// any failure (unreachable host, non-HTML response, blocked target, parse
/// yielding nothing useful) — the caller treats that as "no preview",
/// never as an error worth surfacing to the chat UI.
pub async fn fetch(client: &reqwest::Client, url: &str) -> Option<LinkPreview> {
    let parsed = reqwest::Url::parse(url).ok()?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return None;
    }
    let host = parsed.host_str()?;
    if !host_is_safe(host) {
        return None;
    }

    let resp = client.get(parsed.clone()).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !content_type.is_empty() && !content_type.contains("html") {
        return None;
    }
    if let Some(len) = resp.content_length() {
        if len > MAX_BODY_BYTES {
            return None;
        }
    }

    let html = resp.text().await.ok()?;
    let preview = parse_html(&html, url);
    if preview.title.is_none() && preview.description.is_none() && preview.image.is_none() {
        return None;
    }
    Some(preview)
}
