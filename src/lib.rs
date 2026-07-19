//! # delonix-sdk
//!
//! Cliente Rust **oficial** da API de gestão do **Delonix Runtime**
//! (`delonix-mgmt`, servida por `delonix api` num unix socket).
//!
//! É a peça responsável por um control-plane (ex.: o **Delonix PaaS**) falar com o
//! runtime **pela sua API**, sem link aos crates do motor e sem reimplementar nada:
//! o SDK só fala HTTP+JSON com o socket. Assim o isolamento fica num único sítio — o
//! PaaS aproveita o SDK para enriquecer a sua própria API, em vez de reinventar a
//! roda.
//!
//! O SDK é **auto-contido**: as leituras estruturadas devolvem [`serde_json::Value`]
//! (o chamador desserializa no tipo que quiser) e as operações devolvem
//! [`OpResult`] (`{ok, output}`). Não depende de nenhum crate do runtime nem do PaaS.
//!
//! ```no_run
//! # async fn demo() -> Result<(), delonix_sdk::SdkError> {
//! let rt = delonix_sdk::MgmtClient::connect("unix:///run/delonix-mgmt.sock");
//! rt.ping().await?;
//! let volumes = rt.volumes_list().await?; // serde_json::Value (array)
//! let out = rt.container_run(&serde_json::json!({
//!     "image": "alpine:latest", "name": "web", "command": ["sleep", "60"]
//! })).await?;
//! assert!(out.ok);
//! # Ok(()) }
//! ```

use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::Value;

/// Erro do SDK. `Http` traz o código e a mensagem (`{"error":…}`) da API; `Transport`
/// é uma falha de ligação/tempo-esgotado; `Decode` é JSON malformado da resposta.
#[derive(Debug, Clone)]
pub enum SdkError {
    Transport(String),
    Http { status: u16, message: String },
    Decode(String),
}

impl std::fmt::Display for SdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SdkError::Transport(m) => write!(f, "transporte: {m}"),
            SdkError::Http { status, message } => write!(f, "HTTP {status}: {message}"),
            SdkError::Decode(m) => write!(f, "descodificação: {m}"),
        }
    }
}

impl std::error::Error for SdkError {}

/// Resultado de uma operação de mutação (`{ok, output}`). `ok` é o sucesso da
/// OPERAÇÃO no runtime (não do transporte — uma falha de rede é um [`SdkError`]).
#[derive(Debug, Clone, Deserialize)]
pub struct OpResult {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub output: String,
}

/// Cliente da API de gestão do runtime, sobre um unix socket local.
#[derive(Clone)]
pub struct MgmtClient {
    sock: String,
    timeout: Duration,
}

impl MgmtClient {
    /// Novo cliente contra o socket dado. `url` aceita um caminho (`/run/x.sock`) ou
    /// `unix:///run/x.sock`. Timeout por-pedido de 30 s (ver [`with_timeout`]).
    ///
    /// [`with_timeout`]: MgmtClient::with_timeout
    pub fn connect(url: &str) -> Self {
        let sock = url.strip_prefix("unix://").unwrap_or(url).to_string();
        Self {
            sock,
            timeout: Duration::from_secs(30),
        }
    }

    /// Ajusta o timeout por-pedido (ligar + resposta + corpo).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    // ---- Núcleo HTTP-sobre-unix-socket -------------------------------------

    /// Faz um pedido e devolve `(status, corpo)`. Envolvido num timeout — um runtime
    /// lento/parado nunca prende o chamador indefinidamente.
    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(u16, Vec<u8>), SdkError> {
        let sock = self.sock.clone();
        let exchange = async move {
            let stream = tokio::net::UnixStream::connect(&sock)
                .await
                .map_err(|e| SdkError::Transport(format!("ligar ({sock}): {e}")))?;
            let io = TokioIo::new(stream);
            let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
                .await
                .map_err(|e| SdkError::Transport(format!("handshake: {e}")))?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            let has_body = body.is_some();
            let full = Full::new(Bytes::from(body.unwrap_or_default()));
            let mut builder = hyper::Request::builder()
                .method(method)
                .uri(path)
                .header("host", "localhost");
            if has_body {
                builder = builder.header("content-type", "application/json");
            }
            let req = builder
                .body(full)
                .map_err(|e| SdkError::Transport(format!("pedido: {e}")))?;
            let resp = sender
                .send_request(req)
                .await
                .map_err(|e| SdkError::Transport(format!("enviar: {e}")))?;
            let status = resp.status().as_u16();
            let bytes = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| SdkError::Transport(format!("corpo: {e}")))?
                .to_bytes();
            Ok::<_, SdkError>((status, bytes.to_vec()))
        };
        tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| {
                SdkError::Transport(format!(
                    "sem resposta em {} ms",
                    self.timeout.as_millis()
                ))
            })?
    }

    /// `GET <path>` → `Value` (200), ou `SdkError::Http` com a mensagem `{"error":…}`.
    async fn get_json(&self, path: &str) -> Result<Value, SdkError> {
        let (st, body) = self.request("GET", path, None).await?;
        if st == 200 {
            decode(&body)
        } else {
            Err(http_error(st, &body))
        }
    }

    /// `GET <path>` → `Some(Value)` (200) / `None` (404) / erro. Para recursos que
    /// podem não existir (`get` individual).
    async fn get_opt(&self, path: &str) -> Result<Option<Value>, SdkError> {
        let (st, body) = self.request("GET", path, None).await?;
        match st {
            200 => Ok(Some(decode(&body)?)),
            404 => Ok(None),
            _ => Err(http_error(st, &body)),
        }
    }

    /// `POST/DELETE <path>` de uma OPERAÇÃO → [`OpResult`] (200), ou erro.
    async fn op(&self, method: &str, path: &str, body: Option<Value>) -> Result<OpResult, SdkError> {
        let raw = match body {
            Some(v) => Some(
                serde_json::to_vec(&v).map_err(|e| SdkError::Transport(format!("serializar: {e}")))?,
            ),
            None => None,
        };
        let (st, resp) = self.request(method, path, raw).await?;
        if st == 200 {
            serde_json::from_slice::<OpResult>(&resp)
                .map_err(|e| SdkError::Decode(format!("op: {e}")))
        } else {
            Err(http_error(st, &resp))
        }
    }

    // ---- Superfície da API -------------------------------------------------

    /// Confirma que a API responde (`GET /_ping`).
    pub async fn ping(&self) -> Result<(), SdkError> {
        let (st, _) = self.request("GET", "/_ping", None).await?;
        if st == 200 {
            Ok(())
        } else {
            Err(SdkError::Http {
                status: st,
                message: "ping falhou".into(),
            })
        }
    }

    // Volumes
    pub async fn volumes_list(&self) -> Result<Value, SdkError> {
        self.get_json("/v1/volumes").await
    }
    pub async fn volume_get(&self, name: &str) -> Result<Option<Value>, SdkError> {
        self.get_opt(&format!("/v1/volumes/{}", enc(name))).await
    }
    /// Cria um volume. `body` é o corpo JSON (`{name, driver?, device?, options?}`).
    pub async fn volume_create(&self, body: &Value) -> Result<Value, SdkError> {
        let raw = serde_json::to_vec(body)
            .map_err(|e| SdkError::Transport(format!("serializar: {e}")))?;
        let (st, resp) = self.request("POST", "/v1/volumes", Some(raw)).await?;
        if st == 200 || st == 201 {
            decode(&resp)
        } else {
            Err(http_error(st, &resp))
        }
    }
    /// Apaga um volume (204/200 → `true`, 404 → `false`).
    pub async fn volume_delete(&self, name: &str) -> Result<bool, SdkError> {
        let (st, body) = self
            .request("DELETE", &format!("/v1/volumes/{}", enc(name)), None)
            .await?;
        match st {
            200 | 204 => Ok(true),
            404 => Ok(false),
            _ => Err(http_error(st, &body)),
        }
    }

    // Containers — leitura
    pub async fn containers_list(&self) -> Result<Value, SdkError> {
        self.get_json("/v1/containers").await
    }
    pub async fn container_get(&self, id: &str) -> Result<Option<Value>, SdkError> {
        self.get_opt(&format!("/v1/containers/{}", enc(id))).await
    }
    // Containers — mutação
    /// Arranca um container (`POST /v1/containers`); `spec` é o `ContainerRunSpec` em JSON.
    pub async fn container_run(&self, spec: &Value) -> Result<OpResult, SdkError> {
        self.op("POST", "/v1/containers", Some(spec.clone())).await
    }
    pub async fn container_delete(&self, id: &str, force: bool) -> Result<OpResult, SdkError> {
        let path = format!("/v1/containers/{}?force={}", enc(id), force);
        self.op("DELETE", &path, None).await
    }
    /// `action` ∈ start|stop|restart|pause|unpause|remove.
    pub async fn container_action(&self, id: &str, action: &str) -> Result<OpResult, SdkError> {
        let path = format!("/v1/containers/{}/action", enc(id));
        self.op("POST", &path, Some(serde_json::json!({ "action": action })))
            .await
    }
    pub async fn container_logs(&self, id: &str) -> Result<OpResult, SdkError> {
        self.op("GET", &format!("/v1/containers/{}/logs", enc(id)), None)
            .await
    }
    pub async fn container_exec(&self, id: &str, cmd: &str) -> Result<OpResult, SdkError> {
        let path = format!("/v1/containers/{}/exec", enc(id));
        self.op("POST", &path, Some(serde_json::json!({ "cmd": cmd })))
            .await
    }
    /// Reconfig a quente (portas): `body` = `{publish_add:[], publish_rm:[]}`.
    pub async fn container_reconfig(&self, id: &str, body: &Value) -> Result<OpResult, SdkError> {
        let path = format!("/v1/containers/{}/reconfig", enc(id));
        self.op("POST", &path, Some(body.clone())).await
    }

    // Imagens
    pub async fn images_list(&self) -> Result<Value, SdkError> {
        self.get_json("/v1/images").await
    }
    pub async fn image_delete(&self, reference: &str) -> Result<OpResult, SdkError> {
        // `remove` devolve `{result}`, não `{ok,output}` — adapta-se aqui.
        let (st, body) = self
            .request("DELETE", &format!("/v1/images?ref={}", enc(reference)), None)
            .await?;
        if st == 200 {
            let v: Value = decode(&body)?;
            let out = v
                .get("result")
                .and_then(|r| r.as_str())
                .unwrap_or_default()
                .to_string();
            Ok(OpResult { ok: true, output: out })
        } else {
            let e = http_error(st, &body);
            let msg = match &e {
                SdkError::Http { message, .. } => message.clone(),
                other => other.to_string(),
            };
            Ok(OpResult { ok: false, output: msg })
        }
    }
    pub async fn image_pull(&self, reference: &str, scan_after: bool) -> Result<OpResult, SdkError> {
        let body = serde_json::json!({ "ref": reference, "scan_after": scan_after });
        self.op("POST", "/v1/images/pull", Some(body)).await
    }
    pub async fn image_build(&self, delonixfile: &str, tag: &str) -> Result<OpResult, SdkError> {
        let body = serde_json::json!({ "delonixfile": delonixfile, "tag": tag });
        self.op("POST", "/v1/images/build", Some(body)).await
    }
    pub async fn image_scan(&self, reference: &str) -> Result<OpResult, SdkError> {
        self.op("GET", &format!("/v1/images/scan?ref={}", enc(reference)), None)
            .await
    }
    /// SBOM (`Some(Value)` array de pacotes, `None` se a imagem não existir).
    pub async fn image_sbom(&self, reference: &str) -> Result<Option<Value>, SdkError> {
        self.get_opt(&format!("/v1/images/sbom?ref={}", enc(reference)))
            .await
    }

    // Redes
    pub async fn network_create(&self, name: &str) -> Result<OpResult, SdkError> {
        self.op("POST", "/v1/networks", Some(serde_json::json!({ "name": name })))
            .await
    }
    pub async fn network_delete(&self, name: &str) -> Result<OpResult, SdkError> {
        self.op("DELETE", &format!("/v1/networks/{}", enc(name)), None)
            .await
    }

    // VMs (só stop/rm — o runtime não tem run/start/create para este caminho)
    /// `action` ∈ stop|rm.
    pub async fn vm_action(&self, name: &str, action: &str) -> Result<OpResult, SdkError> {
        let path = format!("/v1/vms/{}/action", enc(name));
        self.op("POST", &path, Some(serde_json::json!({ "action": action })))
            .await
    }
}

/// Percent-encode de um segmento/valor de path/query — impede que um nome com
/// `/`, `:`, `?`, `#`, espaço, … altere a estrutura da URI (o servidor descodifica e
/// valida). Mantém intactos os carateres não-reservados.
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Desserializa um corpo JSON num `Value`.
fn decode(body: &[u8]) -> Result<Value, SdkError> {
    serde_json::from_slice(body).map_err(|e| SdkError::Decode(e.to_string()))
}

/// Constrói um `SdkError::Http` a partir do status + corpo `{"error":…}` (ou cru).
fn http_error(status: u16, body: &[u8]) -> SdkError {
    let message = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
        .unwrap_or_else(|| String::from_utf8_lossy(body).into_owned());
    SdkError::Http { status, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_normaliza_o_prefixo_unix() {
        assert_eq!(MgmtClient::connect("unix:///run/x.sock").sock, "/run/x.sock");
        assert_eq!(MgmtClient::connect("/run/x.sock").sock, "/run/x.sock");
    }

    #[test]
    fn enc_escapa_o_perigoso_mantem_o_valido() {
        assert_eq!(enc("nginx-1.2_v"), "nginx-1.2_v");
        assert_eq!(enc("library/nginx:latest"), "library%2Fnginx%3Alatest");
        assert_eq!(enc("evil?x#y z"), "evil%3Fx%23y%20z");
    }

    #[test]
    fn op_result_desserializa_com_defaults() {
        let r: OpResult = serde_json::from_str(r#"{"ok":true,"output":"feito"}"#).unwrap();
        assert!(r.ok && r.output == "feito");
        let empty: OpResult = serde_json::from_str("{}").unwrap();
        assert!(!empty.ok && empty.output.is_empty());
    }

    #[test]
    fn http_error_extrai_a_mensagem() {
        let e = http_error(404, r#"{"error":"não encontrado"}"#.as_bytes());
        match e {
            SdkError::Http { status, message } => {
                assert_eq!(status, 404);
                assert_eq!(message, "não encontrado");
            }
            _ => panic!("devia ser Http"),
        }
    }
}
