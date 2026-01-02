use crate::config::OidcConfig;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

#[derive(Clone)]
pub struct OidcState {
    config: OidcConfig,
    jwks_cache: Arc<RwLock<JwksCache>>,
}

struct JwksCache {
    keys: Vec<Jwk>,
    last_fetch: Option<Instant>,
}

#[derive(Debug, Deserialize, Clone)]
struct Jwk {
    kty: String,
    #[serde(rename = "use")]
    use_: Option<String>,
    kid: Option<String>,
    n: Option<String>,
    e: Option<String>,
    alg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    iss: String,
    aud: Option<serde_json::Value>,
    exp: usize,
    iat: usize,
    #[serde(default)]
    scope: Option<String>,
    #[serde(flatten)]
    extra: std::collections::HashMap<String, serde_json::Value>,
}

impl OidcState {
    pub fn new(config: OidcConfig) -> Self {
        Self {
            config,
            jwks_cache: Arc::new(RwLock::new(JwksCache {
                keys: Vec::new(),
                last_fetch: None,
            })),
        }
    }

    async fn fetch_jwks(&self) -> Result<Vec<Jwk>, String> {
        let issuer_url = self
            .config
            .issuer_url
            .as_ref()
            .ok_or("OIDC issuer URL not configured")?;

        let discovery_url = format!("{}/.well-known/openid-configuration", issuer_url.trim_end_matches('/'));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        let discovery: OidcDiscovery = client
            .get(&discovery_url)
            .send()
            .await
            .map_err(|e| format!("Failed to fetch OIDC discovery: {}", e))?
            .json()
            .await
            .map_err(|e| format!("Failed to parse OIDC discovery: {}", e))?;

        let jwks: JwksResponse = client
            .get(&discovery.jwks_uri)
            .send()
            .await
            .map_err(|e| format!("Failed to fetch JWKS: {}", e))?
            .json()
            .await
            .map_err(|e| format!("Failed to parse JWKS: {}", e))?;

        Ok(jwks.keys)
    }

    async fn get_jwks(&self) -> Result<Vec<Jwk>, String> {
        let cache = self.jwks_cache.read().await;
        let cache_ttl = Duration::from_secs(self.config.jwks_cache_secs);

        if let Some(last_fetch) = cache.last_fetch {
            if last_fetch.elapsed() < cache_ttl && !cache.keys.is_empty() {
                return Ok(cache.keys.clone());
            }
        }
        drop(cache);

        let keys = self.fetch_jwks().await?;

        let mut cache = self.jwks_cache.write().await;
        cache.keys = keys.clone();
        cache.last_fetch = Some(Instant::now());

        Ok(keys)
    }

    fn find_key(&self, keys: &[Jwk], kid: Option<&str>) -> Option<Jwk> {
        if let Some(kid) = kid {
            keys.iter()
                .find(|k| k.kid.as_deref() == Some(kid))
                .cloned()
        } else {
            keys.first().cloned()
        }
    }

    async fn validate_token(&self, token: &str) -> Result<Claims, String> {
        let header = decode_header(token)
            .map_err(|e| format!("Invalid token header: {}", e))?;

        let keys = self.get_jwks().await?;
        let key = self
            .find_key(&keys, header.kid.as_deref())
            .ok_or("No matching key found in JWKS")?;

        if key.kty != "RSA" {
            return Err(format!("Unsupported key type: {}", key.kty));
        }

        let n = key.n.as_ref().ok_or("Missing 'n' in JWK")?;
        let e = key.e.as_ref().ok_or("Missing 'e' in JWK")?;

        let decoding_key = DecodingKey::from_rsa_components(n, e)
            .map_err(|e| format!("Invalid RSA components: {}", e))?;

        let mut validation = Validation::new(match header.alg {
            Algorithm::RS256 => Algorithm::RS256,
            Algorithm::RS384 => Algorithm::RS384,
            Algorithm::RS512 => Algorithm::RS512,
            _ => return Err(format!("Unsupported algorithm: {:?}", header.alg)),
        });

        if let Some(ref aud) = self.config.audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        if let Some(ref issuer) = self.config.issuer_url {
            validation.set_issuer(&[issuer]);
        }

        let token_data = decode::<Claims>(token, &decoding_key, &validation)
            .map_err(|e| format!("Token validation failed: {}", e))?;

        Ok(token_data.claims)
    }

    fn check_scopes(&self, claims: &Claims) -> bool {
        if self.config.required_scopes.is_empty() {
            return true;
        }

        let token_scopes: HashSet<&str> = claims
            .scope
            .as_ref()
            .map(|s| s.split_whitespace().collect())
            .unwrap_or_default();

        self.config
            .required_scopes
            .iter()
            .all(|required| token_scopes.contains(required.as_str()))
    }

    fn check_roles(&self, claims: &Claims) -> bool {
        if self.config.required_roles.is_empty() {
            return true;
        }

        let token_roles: HashSet<String> = self.extract_roles(claims);

        self.config
            .required_roles
            .iter()
            .all(|required| token_roles.contains(required))
    }

    fn extract_roles(&self, claims: &Claims) -> HashSet<String> {
        let mut roles = HashSet::new();
        let claim_path: Vec<&str> = self.config.roles_claim.split('.').collect();

        let mut current = claims.extra.get(claim_path[0]);
        for part in claim_path.iter().skip(1) {
            current = current.and_then(|v| v.get(part));
        }

        if let Some(value) = current {
            if let Some(arr) = value.as_array() {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        roles.insert(s.to_string());
                    }
                }
            } else if let Some(s) = value.as_str() {
                for role in s.split_whitespace() {
                    roles.insert(role.to_string());
                }
            }
        }

        roles
    }
}

pub async fn oidc_auth_middleware(
    State(state): State<OidcState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    let token = match auth_header {
        Some(h) if h.starts_with("Bearer ") => &h[7..],
        Some(_) => {
            warn!("Invalid Authorization header format");
            return (
                StatusCode::UNAUTHORIZED,
                "Invalid Authorization header format",
            )
                .into_response();
        }
        None => {
            warn!("Missing Authorization header");
            return (StatusCode::UNAUTHORIZED, "Missing Authorization header").into_response();
        }
    };

    match state.validate_token(token).await {
        Ok(claims) => {
            if !state.check_scopes(&claims) {
                warn!("Insufficient scopes for user: {}", claims.sub);
                return (StatusCode::FORBIDDEN, "Insufficient scopes").into_response();
            }

            if !state.check_roles(&claims) {
                warn!("Insufficient roles for user: {}", claims.sub);
                return (StatusCode::FORBIDDEN, "Insufficient roles").into_response();
            }

            debug!("OIDC auth successful for user: {}", claims.sub);
            next.run(request).await
        }
        Err(e) => {
            error!("OIDC validation failed: {}", e);
            (StatusCode::UNAUTHORIZED, "Invalid token").into_response()
        }
    }
}

pub fn create_oidc_state(config: &OidcConfig) -> Option<OidcState> {
    if config.enabled && config.issuer_url.is_some() {
        Some(OidcState::new(config.clone()))
    } else {
        None
    }
}
