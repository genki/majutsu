# S3 互換 provider matrix

| Provider | Put/Get | Range GET | Multipart | Conditional PUT | Tags | Storage class | Lifecycle apply | Archive restore | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| File remote | yes | yes | n/a | yes | n/a | n/a | n/a | n/a | CI E2E |
| MinIO | yes | yes | yes | yes | partial | partial | partial | n/a | docker E2E |
| AWS S3 | yes | yes | yes | yes | yes | yes | yes | yes | manual validation required |
| GCS S3-compatible endpoint | yes | yes | yes | provider-specific | provider-specific | provider-specific | prefer native GCS lifecycle | provider-specific | manual validation required |
| Cloudflare R2 | yes | yes | yes | provider-specific | provider-specific | limited | provider-specific | no Glacier-style restore | manual validation required |

各 release では、この matrix に実際に検証した provider と version を記録する。
