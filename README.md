# delonix-sdk

Cliente Rust **oficial** da API de gestão do [**Delonix Runtime**](https://github.com/angolardevops/delonix-runtime)
(`delonix-mgmt`, servida por `delonix api` sobre um unix socket).

É a peça responsável por um control-plane (ex.: o **Delonix PaaS**) falar com o
runtime **pela sua API** — sem link aos crates do motor e sem reimplementar nada. O
isolamento fica num único sítio: quem consome o runtime aproveita este SDK para
enriquecer a sua própria API, em vez de reinventar a roda.

## Uso

```rust
use delonix_sdk::MgmtClient;

# async fn demo() -> Result<(), delonix_sdk::SdkError> {
let rt = MgmtClient::connect("unix:///run/delonix-mgmt.sock");
rt.ping().await?;

// Leituras estruturadas → serde_json::Value (desserializa no tipo que quiseres).
let volumes = rt.volumes_list().await?;
let containers = rt.containers_list().await?;

// Operações → OpResult { ok, output }.
let run = rt.container_run(&serde_json::json!({
    "image": "alpine:latest", "name": "web", "command": ["sleep", "60"]
})).await?;
assert!(run.ok);

rt.image_pull("alpine:latest", false).await?;
rt.network_create("minha-rede").await?;
# Ok(()) }
```

## Superfície

- **Volumes** — `volumes_list`, `volume_get`, `volume_create`, `volume_delete`
- **Containers** — `containers_list`, `container_get`, `container_run`,
  `container_action`, `container_delete`, `container_logs`, `container_exec`,
  `container_reconfig`
- **Imagens** — `images_list`, `image_delete`, `image_pull`, `image_build`,
  `image_scan`, `image_sbom`
- **Redes** — `network_create`, `network_delete`
- **VMs** — `vm_action` (stop/rm)

## Desenho

- **Auto-contido**: leituras devolvem `serde_json::Value`; operações devolvem
  `OpResult`. Zero dependências de crates do runtime ou do PaaS.
- **HTTP+JSON sobre unix socket** (`hyper` + `tokio`), com timeout por-pedido
  configurável (`with_timeout`, default 30 s).
- Erros tipados (`SdkError`: `Transport` / `Http { status, message }` / `Decode`).

## Licença

Apache-2.0.
