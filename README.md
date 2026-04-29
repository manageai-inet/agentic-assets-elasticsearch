# agentic-assets-elasticsearch

[![GitHub release](https://img.shields.io/github/v/release/manageai-inet/agentic-assets-elasticsearch?label=version)](https://github.com/manageai-inet/agentic-assets-elasticsearch/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/manageai-inet/agentic-assets-elasticsearch/v8.svg)](https://pkg.go.dev/github.com/manageai-inet/agentic-assets-elasticsearch/v8)

Elasticsearch implementation for the [Agentic Assets Manager](https://github.com/manageai-inet/agentic-assets).

This repository hosts the Elasticsearch-backed storage providers used by the agentic-assets ecosystem to manage contextual assets and vector embeddings.

## Modules

| Module | Path | Description |
| --- | --- | --- |
| `v8` | [`./v8`](./v8) | Elasticsearch v8 implementation. See the [module README](./v8/README.md) for details. |

## Installation

```bash
go get github.com/manageai-inet/agentic-assets-elasticsearch/v8
```

## Quick Start

```go
import (
    "github.com/elastic/go-elasticsearch/v8"
    elsam "github.com/manageai-inet/agentic-assets-elasticsearch/v8"
)

cfg := elasticsearch.Config{
    Addresses: []string{"http://localhost:9200"},
    Username:  "elastic",
    Password:  "changeme",
}

client, err := elasticsearch.NewTypedClient(cfg)
if err != nil {
    log.Fatal(err)
}

assets := elsam.NewElasticsearchV8AssetManagerRepo("assets", client)
if err := assets.Setup(ctx); err != nil {
    log.Fatal(err)
}
```

For full usage (asset CRUD, vector storage, similarity search), see [`v8/README.md`](./v8/README.md).

## Development

Requirements:

- Go 1.25+
- A running Elasticsearch v8 instance (default: `http://localhost:9200`) for integration tests

Run tests:

```bash
cd v8
go test ./...
```

## Releasing

Tag a commit on `main` with a `v*` tag (e.g. `v8.0.1`) and push the tag. The `Release` workflow generates a GitHub release with auto-populated notes.

```bash
git tag v8.0.1
git push origin v8.0.1
```

## License

[MIT](LICENSE) &copy; ManageAI
