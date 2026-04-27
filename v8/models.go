package v8

import (
	"time"

	am "github.com/manageai-inet/agentic-assets"
)

type ElasticAssetDocument struct {
	am.ContextualAsset
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ElasticVectorAssetDocument struct {
	am.VectorAsset
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

const (
	kbIdKey           = "kb_id"
	assetTypeKey      = "asset_type"
	assetIdKey        = "asset_id"
	contentKey        = "content"
	embeddingModelKey = "embedding_model"
	embededVectorKey  = "embeded_vector"
	labelsKey         = "labels"
	metadataKey       = "metadata"
	createdAtKey      = "created_at"
	updatedAtKey      = "updated_at"
	refsKey           = "refs"
	refTypeKey        = "ref_type"
	versionKey        = "version"
)
