# agentic-assets-elasticsearch/v8

Elasticsearch v8 implementation for the Agentic Assets Manager.

This package provides Elasticsearch-based storage implementations for managing assets and vector embeddings as part of the agentic-assets ecosystem.

## Features

- **Asset Management**: Store, retrieve, and manage contextual assets with versioning
- **Vector Storage**: Store and search vector embeddings with similarity search
- **Elasticsearch v8 Support**: Built on the official Elasticsearch Go client v8
- **Logging Integration**: Integrated logging using Go's slog package
- **Automatic Index Management**: Automatically creates and configures indices with proper mappings

## Installation

```bash
go get github.com/manageai-inet/agentic-assets-elasticsearch/v8
```

## Usage

### Basic Setup

```go
import (
    // ... other imports
    "github.com/elastic/go-elasticsearch/v8"
    elsam "github.com/manageai-inet/agentic-assets-elasticsearch/v8"
)

elsCfg := elasticsearch.Config{
    Addresses: []string{"http://localhost:9200"},
    Username:  "your-username",    // optional
    Password:  "your-password",    // optional
    APIKey:    "your-api-key",     // optional
    ServiceToken: "your-service-token", // optional
}

elsCli, err := elasticsearch.NewTypedClient(elsCfg)
if err != nil {
    logger.Error("Failed to initialize Elasticsearch Client", slog.Any("error", err))
    os.Exit(1)
}
```

### Asset Storage

Initialize and use the asset storage manager:

```go
logger.Debug("Initializing Asset Storage", slog.String("index", "assets"))
assetStorage := elsam.NewElasticsearchV8AssetManagerRepo("assets", elsCli)
assetStorage.SetLogger(logger)

err = assetStorage.Setup(ctx)
if err != nil {
    logger.Error("Failed to initialize Asset Storage", slog.Any("error", err))
    os.Exit(1)
}
```

### Vector Storage

Initialize and use the vector storage manager:

```go
logger.Debug("Initializing Vector Storage", slog.String("index", "vectors"))
vectorStorage := elsam.NewElasticsearchV8VectorRepo("vectors", elsCli, embedder, "") // empty similarity uses cosine by default
vectorStorage.SetLogger(logger)

err = vectorStorage.Setup(ctx)
if err != nil {
    logger.Error("Failed to initialize Vector Storage", slog.Any("error", err))
    os.Exit(1)
}
```

## API Overview

### Asset Manager

- `NewElasticsearchV8AssetManagerRepo(index, client)`: Create a new asset manager
- `Setup(ctx)`: Initialize the index and mappings
- `SetLogger(logger)`: Configure logging
- Asset CRUD operations: Store, Retrieve, Update, Delete assets with versioning

### Vector Repository

- `NewElasticsearchV8VectorRepo(index, client, embedder, similarity)`: Create a new vector repository
- `Setup(ctx)`: Initialize the index with vector mappings
- `SetLogger(logger)`: Configure logging
- `SetEmbedder(ctx, embedder)`: Configure the embedding function
- Vector operations: Store vectors, similarity search, etc.

## Dependencies

- [github.com/elastic/go-elasticsearch/v8](https://github.com/elastic/go-elasticsearch/v8)
- [github.com/manageai-inet/agentic-assets](https://github.com/manageai-inet/agentic-assets)

## License

MIT License - see [LICENSE](../LICENSE) for details.