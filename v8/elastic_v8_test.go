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

type TestConfig struct {
	Host        string
	User        string
	Password    string
	AssetsIndex string
	VectorIndex string
}

func getTestConfig() TestConfig {
	return TestConfig{
		Host:        "http://localhost:9200",
		User:        "elastic",
		Password:    "Admin123!",
		AssetsIndex: "test_index",
		VectorIndex: "test_vector_index",
	}
}

func getTestElasticsearchV8AssetManagerRepo() (*ElasticsearchV8AssetManagerRepo, error) {
	cfg := getTestConfig()
	esConfig := els.Config{
		Addresses: []string{cfg.Host},
		Username:  cfg.User,
		Password:  cfg.Password,
	}
	es, err := els.NewTypedClient(esConfig)
	if err != nil {
		return nil, err
	}
	logger := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	repo := NewElasticsearchV8AssetManagerRepo(
		cfg.AssetsIndex, es,
	)
	am.SetLogger(repo, logger)
	return repo, nil
}

func TestElasticsearchV8AssetManagerRepo_Setup(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	// Verify the repository was created with correct index
	if repo.assets_index != "test_index" {
		t.Errorf("Expected assets_index to be 'test_index', got '%s'", repo.assets_index)
	}

	// Test that Setup method exists and can be called
	err = repo.Setup(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	t.Log("Setup method test passed")
}

func TestElasticsearchV8AssetManagerRepo_InsertAsset(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	assetId := fmt.Sprintf("%s:test:0", kbId)
	asset := am.ContextualAsset{
		KbId:      kbId,
		AssetType: "test",
		AssetId:   assetId,
		Version:   1,
		Content:   "This is a test content",
		Labels:    &[]string{"testing"},
	}
	inserted, err := repo.InsertAsset(ctx, asset)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !inserted {
		t.Errorf("Expected inserted to be true, got false")
	}
	t.Log("InsertAsset method test passed")
}

func TestElasticsearchV8AssetManagerRepo_InsertAssets(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	version := 2
	assets := []am.ContextualAsset{
		{
			KbId:      "kbId-insert-test-01234567890",
			AssetType: "test",
			AssetId:   "kbId-insert-test-01234567890:test:0",
			Version:   version,
			Content:   "This is a test content of asset 0",
			Labels:    &[]string{"testing"},
		},
		{
			KbId:      "kbId-insert-test-01234567890",
			AssetType: "test",
			AssetId:   "kbId-insert-test-01234567890:test:1",
			Version:   version,
			Content:   "This is a test content of asset 1",
			Labels:    &[]string{"testing"},
		},
		// with refs
		{
			KbId:      "kbId-insert-test-01234567890",
			AssetType: "test",
			AssetId:   "kbId-insert-test-01234567890:test:2",
			Version:   version,
			Content:   "This is a test content of asset 2, my parent is asset 0",
			Labels:    &[]string{"testing"},
			Refs: &[]am.AssetRef{
				{
					KbId:      "kbId-insert-test-01234567890",
					AssetType: "test",
					AssetId:   "kbId-insert-test-01234567890:test:0",
					RefType:   "parent",
				},
			},
		},
	}
	success, err := repo.InsertBatchAssets(ctx, assets)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if success != len(assets) {
		t.Errorf("Expected success to be %d, got %d", len(assets), success)
	}
	t.Log("InsertAssets method test passed")
}

func TestElasticsearchV8AssetManagerRepo_GetVersions(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	// Test that GetVersions method exists (we won't call it to avoid connection issues)
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	versions, err := repo.GetVersions(ctx, kbId)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if versions == nil {
		t.Errorf("Expected versions to be non-nil, got nil")
	}

	// This test ensures the method signature is correct and can be compiled
	t.Log("GetVersions method signature test passed", slog.Any("versions", versions))
}

func TestElasticsearchV8AssetManagerRepo_GetLatestVersion(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	latestVersion, err := repo.getLatestVersions(ctx, kbId)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if latestVersion == nil {
		t.Errorf("Expected latestVersion to be non-nil, got nil")
	}
	t.Log("GetLatestVersion method test passed", slog.Int("latestVersion", *latestVersion))
}

func TestElasticsearchV8AssetManagerRepo_GetAssets(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	assets, err := repo.GetAssets(ctx, kbId, "test", nil, nil, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if assets == nil {
		t.Errorf("Expected assets to be non-nil, got nil")
	}
	t.Log("GetAssets method test passed", slog.Int("assets", len(assets)))
	for _, asset := range assets {
		t.Log("Asset", slog.Any("asset", asset.AssetId))
	}
}

func TestElasticsearchV8AssetManagerRepo_GetAssetsWithVersion(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	version := 1
	assets, err := repo.GetAssets(ctx, kbId, "test", nil, &version, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if assets == nil {
		t.Errorf("Expected assets to be non-nil, got nil")
	}
	t.Log("GetAssets method test passed", slog.Int("assets", len(assets)))
	for _, asset := range assets {
		t.Log("Asset", slog.Any("asset", asset.AssetId))
	}
}

func TestElasticsearchV8AssetManagerRepo_GetAssetsByAssetIds(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	assetIds := []string{"kbId-insert-test-01234567890:test:0", "kbId-insert-test-01234567890:test:2"}
	assets, err := repo.GetAssets(ctx, kbId, "test", &assetIds, nil, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if assets == nil {
		t.Errorf("Expected assets to be non-nil, got nil")
	}
	t.Log("GetAssetsByAssetIds method test passed", slog.Int("assets", len(assets)))
	for _, asset := range assets {
		t.Log("Asset", slog.Any("asset", asset.AssetId))
	}
}

func TestElasticsearchV8AssetManagerRepo_GetAssetsByRefs(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	refIds := []string{"kbId-insert-test-01234567890:test:0"}
	assets, err := repo.GetAssetsByRefs(ctx, kbId, "test", &refIds, &[]string{"parent"}, nil, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if assets == nil {
		t.Errorf("Expected assets to be non-nil, got nil")
	}
	t.Log("GetAssetsByRefs method test passed", slog.Int("assets", len(assets)))
	for _, asset := range assets {
		t.Log("Asset", slog.Any("asset", asset.AssetId), slog.Int("refs", len(*asset.Refs)))
		for _, ref := range *asset.Refs {
			t.Log("- Ref", slog.Any("ref", ref.AssetId), slog.Any("refType", ref.RefType))
		}
	}
}

func TestElasticsearchV8AssetManagerRepo_CheckAssetsExist(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	assetIds := []string{
		"kbId-insert-test-01234567890:test:0",
		"kbId-insert-test-01234567890:test:1",
		"kbId-insert-test-01234567890:test:2",
		"kbId-insert-test-01234567890:test:3",
	}
	exist, err := repo.CheckAssetsExist(ctx, kbId, "test", assetIds)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	t.Log("CheckAssetsExist method test passed")
	for assetId, isExist := range exist {
		t.Log("Asset", slog.Any("asset", assetId), slog.Bool("exist", isExist))
	}
}

func TestElasticsearchV8AssetManagerRepo_DeleteAsset(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	assetId := "kbId-insert-test-01234567890:test:0"
	version := 1
	deleted, err := repo.DeleteAsset(ctx, kbId, "test", assetId, &version)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !deleted {
		t.Errorf("Expected deleted to be true, got false")
	}
	t.Log("DeleteAsset method test passed", slog.Bool("deleted", deleted))
}

func TestElasticsearchV8AssetManagerRepo_DeleteAssetByKbId(t *testing.T) {
	repo, err := getTestElasticsearchV8AssetManagerRepo()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	ctx := context.Background()
	kbId := "kbId-insert-test-01234567890"
	deleted, err := repo.DeleteAssetsByKbId(ctx, kbId)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if deleted == 0 {
		t.Errorf("Expected deleted to be true, got false")
	}
	t.Log("DeleteAssetByKbId method test passed", slog.Int("deleted", deleted))
}