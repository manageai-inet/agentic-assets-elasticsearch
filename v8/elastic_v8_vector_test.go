package v8

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	am "github.com/manageai-inet/agentic-assets"
	els "github.com/elastic/go-elasticsearch/v8"
)

func getTestElasticsearchV8VectorRepo() (*ElasticsearchV8VectorRepo, error) {
	// Reference the existing test config
	cfg := TestConfig{
		Host:        "http://localhost:9200",
		User:        "elastic",
		Password:    "Admin123!",
		VectorIndex: "test_vector_index",
	}
	esConfig := els.Config{
		Addresses: []string{cfg.Host},
		Username:  cfg.User,
		Password:  cfg.Password,
	}
	es, err := els.NewTypedClient(esConfig)
	if err != nil {
		return nil, err
	}
	
	// Create fake embedder
	fakeEmbedder := am.NewFakeEmbedder("test-model", 128)
	
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	
	repo := NewElasticsearchV8VectorRepo(
		cfg.VectorIndex, 
		es,
		fakeEmbedder,
		"cosine",
	)
	am.SetLogger(repo, logger)
	return repo, nil
}

func TestElasticsearchV8VectorRepo_Setup(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	
	// Verify the repository was created with correct index
	if repo.vectors_index != "test_vector_index" {
		t.Errorf("Expected vectors_index to be 'test_vector_index', got '%s'", repo.vectors_index)
	}

	// Test that Setup method exists and can be called
	err = repo.Setup(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	t.Log("Setup method test passed")
}

func TestElasticsearchV8VectorRepo_EmbedContent(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	content := "This is a test content for embedding"
	
	vector, err := repo.EmbedContent(ctx, content)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if vector == nil {
		t.Errorf("Expected vector to be non-nil, got nil")
	}
	
	if len(vector) != 128 {
		t.Errorf("Expected vector dimension to be 128, got %d", len(vector))
	}
	
	t.Log("EmbedContent method test passed", slog.Int("vectorDim", len(vector)))
}

func TestElasticsearchV8VectorRepo_EmbedAsset(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	
	asset := &am.ContextualAsset{
		KbId:      "kbId-vector-test-01234567890",
		AssetType: "test",
		AssetId:   "kbId-vector-test-01234567890:test:0",
		Version:   1,
		Content:   "This is a test content for vector asset",
		Labels:    &[]string{"testing", "vector"},
	}
	
	vectorAsset, err := repo.EmbedAsset(ctx, asset, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if vectorAsset.KbId != asset.KbId {
		t.Errorf("Expected KbId to be '%s', got '%s'", asset.KbId, vectorAsset.KbId)
	}
	
	if vectorAsset.AssetId != asset.AssetId {
		t.Errorf("Expected AssetId to be '%s', got '%s'", asset.AssetId, vectorAsset.AssetId)
	}
	
	if vectorAsset.Version != asset.Version {
		t.Errorf("Expected Version to be %d, got %d", asset.Version, vectorAsset.Version)
	}
	
	if vectorAsset.Content != asset.Content {
		t.Errorf("Expected Content to be '%s', got '%s'", asset.Content, vectorAsset.Content)
	}
	
	if vectorAsset.EmbeddingModel == nil {
		t.Errorf("Expected EmbeddingModel to be non-nil")
	}
	
	if *vectorAsset.EmbeddingModel != "test-model" {
		t.Errorf("Expected EmbeddingModel to be 'test-model', got '%s'", *vectorAsset.EmbeddingModel)
	}
	
	if vectorAsset.EmbededVector == nil {
		t.Errorf("Expected EmbededVector to be non-nil")
	}
	
	if len(vectorAsset.EmbededVector) != 128 {
		t.Errorf("Expected EmbededVector dimension to be 128, got %d", len(vectorAsset.EmbededVector))
	}
	
	t.Log("EmbedAsset method test passed")
}

func TestElasticsearchV8VectorRepo_InsertVector(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	
	// First embed an asset to create a vector asset
	asset := &am.ContextualAsset{
		KbId:      "kbId-vector-test-01234567890",
		AssetType: "test",
		AssetId:   "kbId-vector-test-01234567890:test:1",
		Version:   1,
		Content:   "This is a test content for inserting vector",
		Labels:    &[]string{"testing", "insert"},
	}
	
	vectorAsset, err := repo.EmbedAsset(ctx, asset, nil)
	if err != nil {
		t.Errorf("Expected no error embedding asset, got %v", err)
	}
	
	inserted, err := repo.InsertVector(ctx, vectorAsset)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if !inserted {
		t.Errorf("Expected inserted to be true, got false")
	}
	
	t.Log("InsertVector method test passed")
}

func TestElasticsearchV8VectorRepo_InsertBatchVectors(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	
	// Create multiple vector assets
	vectorAssets := []am.VectorAsset{}
	
	for i := 0; i < 3; i++ {
		asset := &am.ContextualAsset{
			KbId:      "kbId-vector-test-01234567890",
			AssetType: "test",
			AssetId:   fmt.Sprintf("kbId-vector-test-01234567890:test:batch:%d", i),
			Version:   2,
			Content:   fmt.Sprintf("This is a test content for batch vector %d", i),
			Labels:    &[]string{"testing", "batch"},
		}
		
		vectorAsset, err := repo.EmbedAsset(ctx, asset, nil)
		if err != nil {
			t.Errorf("Expected no error embedding asset, got %v", err)
		}
		
		vectorAssets = append(vectorAssets, vectorAsset)
	}
	
	success, err := repo.InsertBatchVectors(ctx, vectorAssets)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if success != len(vectorAssets) {
		t.Errorf("Expected success count to be %d, got %d", len(vectorAssets), success)
	}
	
	t.Log("InsertBatchVectors method test passed", slog.Int("success", success))
}

func TestElasticsearchV8VectorRepo_GetVersions(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-vector-test-01234567890"
	
	versions, err := repo.GetVersions(ctx, kbId)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if versions == nil {
		t.Errorf("Expected versions to be non-nil, got nil")
	}
	
	t.Log("GetVersions method test passed", slog.Any("versions", versions))
}

func TestElasticsearchV8VectorRepo_QueryVectors(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	
	// Create a query vector by embedding some content
	queryContent := "This is a query content for testing vector search"
	queryVector, err := repo.EmbedContent(ctx, queryContent)
	if err != nil {
		t.Errorf("Expected no error embedding query content, got %v", err)
	}
	
	topK := 5
	threshold := float32(0.1)
	filter := &am.VectorQueryFilter{
		KbId: nil,
		KbIdsIn: []string{"kbId-vector-test-01234567890"},
		Version: nil,
		Label: nil,
	}
	
	results, err := repo.QueryVectors(ctx, queryVector, &topK, &threshold, filter)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if results == nil {
		t.Errorf("Expected results to be non-nil, got nil")
	}
	
	t.Log("QueryVectors method test passed", slog.Int("results", len(results)))
	
	for i, result := range results {
		t.Log(fmt.Sprintf("Result %d", i), slog.String("assetId", result.AssetId), slog.Any("score", *result.Score))
	}
}

func TestElasticsearchV8VectorRepo_DeleteVector(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-vector-test-01234567890"
	assetId := "kbId-vector-test-01234567890:test:1"
	version := 1
	
	deleted, err := repo.DeleteVector(ctx, kbId, assetId, &version)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	// deleted could be true or false depending on whether the vector existed
	t.Log("DeleteVector method test passed", slog.Bool("deleted", deleted))
}

func TestElasticsearchV8VectorRepo_DeleteVectorsByKbId(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-vector-test-01234567890"
	
	deleted, err := repo.DeleteVectorsByKbId(ctx, kbId)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	// deleted could be 0 or more depending on how many vectors existed
	t.Log("DeleteVectorsByKbId method test passed", slog.Int("deleted", deleted))
}

func TestElasticsearchV8VectorRepo_DeleteVectorsByKbIdAndVersion(t *testing.T) {
	repo, err := getTestElasticsearchV8VectorRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-vector-test-01234567890"
	version := 2
	
	deleted, err := repo.DeleteVectorsByKbIdAndVersion(ctx, kbId, &version)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	// deleted could be 0 or more depending on how many vectors existed
	t.Log("DeleteVectorsByKbIdAndVersion method test passed", slog.Int("deleted", deleted))
}