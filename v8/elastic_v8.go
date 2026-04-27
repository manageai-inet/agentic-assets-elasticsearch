package v8

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/densevectorsimilarity"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/healthstatus"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/result"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scriptlanguage"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	am "github.com/manageai-inet/agentic-assets"
)

type ElasticsearchV8AssetManagerRepo struct {
	assets_index string
	es           *elasticsearch.TypedClient
	am.LoggingCapacity
}

func NewElasticsearchV8AssetManagerRepo(assets_index string, es *elasticsearch.TypedClient) *ElasticsearchV8AssetManagerRepo {
	return &ElasticsearchV8AssetManagerRepo{
		assets_index:    assets_index,
		es:              es,
		LoggingCapacity: *am.GetDefaultLoggingCapacity(),
	}
}

func (e *ElasticsearchV8AssetManagerRepo) Setup(ctx context.Context) error {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Setting up Elasticsearch Asset Manager Repo", slog.String("assets_index", e.assets_index))

	logger.Debug("Checking if index exists", slog.String("assets_index", e.assets_index))
	exist, err := e.es.Indices.Exists(e.assets_index).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to check if index exists: "+err.Error(), slog.String("assets_index", e.assets_index))
		return err
	}

	if !exist {
		logger.InfoContext(ctx, "Index not found, creating index", slog.String("assets_index", e.assets_index))
		mapping := types.TypeMapping{
			Properties: map[string]types.Property{
				kbIdKey:           types.KeywordProperty{},
				assetTypeKey:      types.KeywordProperty{},
				assetIdKey:        types.KeywordProperty{},
				versionKey:        types.LongNumberProperty{},
				contentKey:        types.TextProperty{},
				embeddingModelKey: types.KeywordProperty{},
				refsKey: types.NestedProperty{
					Properties: map[string]types.Property{
						kbIdKey:      types.KeywordProperty{},
						assetTypeKey: types.KeywordProperty{},
						assetIdKey:   types.KeywordProperty{},
						refTypeKey:   types.KeywordProperty{},
					},
				},
				labelsKey:    types.KeywordProperty{},
				metadataKey:  types.ObjectProperty{},
				createdAtKey: types.DateProperty{},
				updatedAtKey: types.DateProperty{},
			},
		}

		logger.Debug("Creating index", slog.String("assets_index", e.assets_index))
		res, err := e.es.Indices.Create(e.assets_index).Mappings(&mapping).Do(ctx)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to create index: "+err.Error(), slog.String("assets_index", e.assets_index))
			return err
		}
		if !res.Acknowledged {
			logger.WarnContext(
				ctx,
				"Index creation not acknowledged (may be due to request timeout before the Master node could confirm the update), but does not necessarily mean the index creation failed, please ensure the index is healthy",
				slog.String("assets_index", e.assets_index),
			)
		}
	}
	// ensure index is healthy
	logger.Debug("Checking index health", slog.String("assets_index", e.assets_index))
	healthResp, err := e.es.Cluster.Health().Index(e.assets_index).WaitForStatus(healthstatus.Yellow).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to wait for index health: "+err.Error(), slog.String("assets_index", e.assets_index))
		return err
	}
	logger.InfoContext(ctx, "Elasticsearch Asset Manager Repo setup complete", slog.String("assets_index", e.assets_index), slog.String("status", healthResp.Status.String()))
	return nil
}

func (e *ElasticsearchV8AssetManagerRepo) GetVersions(ctx context.Context, kbId string) ([]int, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Getting versions for kbId", slog.String("kbId", kbId))

	getVersionsQueryRequest := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{
						Term: map[string]types.TermQuery{
							kbIdKey: {Value: kbId},
						},
					},
				},
			},
		},
		Aggregations: map[string]types.Aggregations{
			"versions": {
				Terms: &types.TermsAggregation{
					Field: some.String(versionKey),
					Size:  some.Int(10000),
					Order: map[string]sortorder.SortOrder{
						"_key": sortorder.Asc,
					},
				},
			},
		},
	}

	logger.DebugContext(ctx, "Search assets in Elasticsearch", slog.String("index", e.assets_index), slog.String("kbId", kbId))
	resp, err := e.es.Search().Index(e.assets_index).Request(&getVersionsQueryRequest).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to search for asset aggregations: "+err.Error(), slog.String("kbId", kbId))
		return []int{}, err
	}
	agg := resp.Aggregations["versions"].(*types.LongTermsAggregate)
	if agg.Buckets == nil {
		logger.WarnContext(ctx, "No versions found for kbId", slog.String("kbId", kbId))
		return []int{}, nil
	}
	buckets, ok := agg.Buckets.([]types.LongTermsBucket)
	if !ok {
		err := fmt.Errorf("failed to get asset aggregations: %v", agg.Buckets)
		logger.ErrorContext(ctx, "Failed to get asset aggregations: "+err.Error(), slog.String("kbId", kbId))
		return []int{}, err
	}
	versions := []int{}
	for _, b := range buckets {
		versions = append(versions, int(b.Key))
	}
	return versions, nil
}

func (e *ElasticsearchV8AssetManagerRepo) getLatestVersions(ctx context.Context, kbId string) (*int, error) {
	logger := am.GetLogger(e)
	logger.Debug("Getting latest versions for kbId", slog.String("kbId", kbId))
	aggQuery := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{
						Term: map[string]types.TermQuery{
							kbIdKey: {Value: kbId},
						},
					},
				},
			},
		},
		Aggregations: map[string]types.Aggregations{
			"latest_version": {
				Max: &types.MaxAggregation{
					Field: some.String(versionKey),
				},
			},
		},
		Size: some.Int(0),
	}

	logger.DebugContext(ctx, "Search assets in Elasticsearch", slog.String("index", e.assets_index), slog.String("kbId", kbId))
	resp, err := e.es.Search().Index(e.assets_index).Request(&aggQuery).Do(ctx)
	if err != nil {
		logger.Error("Failed to search for asset aggregations: "+err.Error(), slog.String("kbId", kbId))
		return nil, err
	}
	agg := resp.Aggregations["latest_version"].(*types.MaxAggregate)
	if agg.Value == nil {
		err := fmt.Errorf("no latest version found for kbId: %s", kbId)
		logger.ErrorContext(ctx, "No latest version found", slog.String("kbId", kbId))
		return nil, err
	}
	latestVersion := int(*agg.Value)
	logger.Debug("Getting latest versions for kbId complete", slog.String("kbId", kbId), slog.Int("latestVersion", latestVersion))
	return &latestVersion, nil
}

func (e *ElasticsearchV8AssetManagerRepo) GetAssets(ctx context.Context, kbId string, assetType string, assetIds *[]string, version *int, label *string) ([]am.ContextualAsset, error) {
	logger := am.GetLogger(e)
	logargs := []any{
		slog.String("kbId", kbId),
		slog.String("assetType", assetType),
	}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	if label != nil {
		logargs = append(logargs, slog.String("label", *label))
	} else {
		logargs = append(logargs, slog.String("label", "*"))
	}
	if assetIds != nil {
		logargs = append(logargs, slog.Int("assetIds", len(*assetIds)))
	} else {
		logargs = append(logargs, slog.String("assetIds", "[*]"))
	}
	logger.InfoContext(ctx, "Getting assets for kbId", logargs...)
	if kbId == "" {
		return []am.ContextualAsset{}, fmt.Errorf("kbId is required")
	}
	if assetType == "" {
		return []am.ContextualAsset{}, fmt.Errorf("assetType is required")
	}
	if version == nil {
		logger.DebugContext(ctx, "Getting latest versions for kbId because version is not specified", slog.String("kbId", kbId))
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions for kbId: "+err.Error(), slog.String("kbId", kbId))
			return []am.ContextualAsset{}, err
		}

		if latestVersion == nil {
			logger.WarnContext(ctx, "No latest versions found", slog.String("kbId", kbId))
			return []am.ContextualAsset{}, nil
		}
		version = latestVersion
	}

	query := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
					{Term: map[string]types.TermQuery{assetTypeKey: {Value: assetType}}},
					{Term: map[string]types.TermQuery{versionKey: {Value: *version}}},
				},
			},
		},
		Size: some.Int(10000),
	}

	if label != nil {
		query.Query.Bool.Must = append(
			query.Query.Bool.Must,
			types.Query{Term: map[string]types.TermQuery{labelsKey: {Value: *label}}},
		)
	}
	if assetIds != nil && len(*assetIds) > 0 {
		termFields := []types.TermsQueryField{}
		for _, assetId := range *assetIds {
			termFields = append(termFields, some.String(assetId))
		}
		query.Query.Bool.Must = append(
			query.Query.Bool.Must,
			types.Query{Terms: &types.TermsQuery{
				TermsQuery: map[string]types.TermsQueryField{
					assetIdKey: termFields,
				},
			}},
		)
	}

	logger.DebugContext(ctx, "Search assets in Elasticsearch", append([]any{slog.String("index", e.assets_index)}, logargs...)...)
	resp, err := e.es.Search().Index(e.assets_index).Request(&query).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to search for asset: "+err.Error(), slog.String("kbId", kbId))
		return []am.ContextualAsset{}, err
	}
	assets := []am.ContextualAsset{}
	for _, hit := range resp.Hits.Hits {
		jsonAsset, err := hit.Source_.MarshalJSON()
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal asset: "+err.Error(), slog.String("kbId", kbId))
			return []am.ContextualAsset{}, err
		}
		var asset ElasticAssetDocument
		json.Unmarshal(jsonAsset, &asset)
		assets = append(assets, asset.ContextualAsset)
	}
	return assets, nil
}

func (e *ElasticsearchV8AssetManagerRepo) GetAssetsByRefs(ctx context.Context, kbId string, assetType string, refIds *[]string, refTypes *[]string, version *int, label *string) ([]am.ContextualAsset, error) {
	logger := am.GetLogger(e)
	logargs := []any{
		slog.String("kbId", kbId),
		slog.String("assetType", assetType),
	}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	if label != nil {
		logargs = append(logargs, slog.String("label", *label))
	} else {
		logargs = append(logargs, slog.String("label", "*"))
	}
	if refIds != nil {
		logargs = append(logargs, slog.Int("refIds", len(*refIds)))
	} else {
		logargs = append(logargs, slog.String("refIds", "[*]"))
	}
	if refTypes != nil {
		logargs = append(logargs, slog.Any("refTypes", *refTypes))
	} else {
		logargs = append(logargs, slog.String("refTypes", "[*]"))
	}
	logger.InfoContext(ctx, "Getting assets by refs for kbId", logargs...)
	if kbId == "" {
		return []am.ContextualAsset{}, fmt.Errorf("kbId is required")
	}
	if assetType == "" {
		return []am.ContextualAsset{}, fmt.Errorf("assetType is required")
	}
	if version == nil {
		logger.DebugContext(ctx, "Getting latest versions for kbId because version is not specified", slog.String("kbId", kbId))
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions for kbId: "+err.Error(), slog.String("kbId", kbId))
			return []am.ContextualAsset{}, err
		}

		if latestVersion == nil {
			logger.WarnContext(ctx, "No latest versions found", slog.String("kbId", kbId))
			return []am.ContextualAsset{}, nil
		}
		version = latestVersion
	}
	query := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
					{Term: map[string]types.TermQuery{assetTypeKey: {Value: assetType}}},
					{Term: map[string]types.TermQuery{versionKey: {Value: *version}}},
				},
			},
		},
		Size: some.Int(10000),
	}
	if label != nil {
		query.Query.Bool.Filter = append(
			query.Query.Bool.Filter,
			types.Query{Term: map[string]types.TermQuery{labelsKey: {Value: *label}}},
		)
	}
	if refIds != nil && len(*refIds) > 0 {
		termFields := []types.TermsQueryField{}
		for _, refId := range *refIds {
			termFields = append(termFields, some.String(refId))
		}
		query.Query.Bool.Filter = append(
			query.Query.Bool.Filter,
			types.Query{
				Nested: &types.NestedQuery{
					Path: refsKey,
					Query: types.Query{
						Bool: &types.BoolQuery{
							Must: []types.Query{
								{
									Terms: &types.TermsQuery{
										TermsQuery: map[string]types.TermsQueryField{
											fmt.Sprintf("%s.%s", refsKey, assetIdKey): termFields,
										},
									},
								},
							},
						},
					},
				},
			},
		)
	}
	if refTypes != nil && len(*refTypes) > 0 {
		termFields := []types.TermsQueryField{}
		for _, refType := range *refTypes {
			termFields = append(termFields, some.String(refType))
		}
		query.Query.Bool.Filter = append(
			query.Query.Bool.Filter,
			types.Query{
				Nested: &types.NestedQuery{
					Path: refsKey,
					Query: types.Query{
						Bool: &types.BoolQuery{
							Must: []types.Query{
								{
									Terms: &types.TermsQuery{
										TermsQuery: map[string]types.TermsQueryField{
											fmt.Sprintf("%s.%s", refsKey, refTypeKey): termFields,
										},
									},
								},
							},
						},
					},
				},
			},
		)
	}

	logger.DebugContext(ctx, "Search assets in Elasticsearch", append([]any{slog.String("index", e.assets_index)}, logargs...)...)
	resp, err := e.es.Search().Index(e.assets_index).Request(&query).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to search: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType))
		return []am.ContextualAsset{}, err
	}
	assets := []am.ContextualAsset{}
	for _, hit := range resp.Hits.Hits {
		jsonAsset, err := hit.Source_.MarshalJSON()
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal asset: "+err.Error(), slog.String("kbId", kbId))
			return []am.ContextualAsset{}, err
		}
		var asset ElasticAssetDocument
		json.Unmarshal(jsonAsset, &asset)
		assets = append(assets, asset.ContextualAsset)
	}
	return assets, nil
}

// CheckAssetsExist checks multiple asset IDs at once and returns a map indicating which ones already exist.
func (e *ElasticsearchV8AssetManagerRepo) CheckAssetsExist(ctx context.Context, kbId string, assetType string, assetIds []string) (map[string]bool, error) {
	logger := am.GetLogger(e)
	logargs := []any{
		slog.String("kbId", kbId),
		slog.String("assetType", assetType),
		slog.Int("assetIds", len(assetIds)),
	}
	logger.InfoContext(ctx, "Checking if assets exist", logargs...)
	existsMap := make(map[string]bool)
	if kbId == "" {
		return existsMap, fmt.Errorf("kbId is required")
	}
	if assetType == "" {
		return existsMap, fmt.Errorf("assetType is required")
	}
	if len(assetIds) == 0 {
		logger.WarnContext(ctx, "No asset IDs provided", logargs...)
		return existsMap, nil
	}
	latestVersion, err := e.getLatestVersions(ctx, kbId)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get latest versions: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType))
		return nil, err
	}
	if latestVersion == nil {
		logger.WarnContext(ctx, "No latest versions found", slog.String("kbId", kbId), slog.String("assetType", assetType))
		return existsMap, nil
	}
	termFields := []types.TermsQueryField{}
	for _, assetId := range assetIds {
		existsMap[assetId] = false
		termFields = append(termFields, some.String(assetId))
	}
	query := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
					{Term: map[string]types.TermQuery{assetTypeKey: {Value: assetType}}},
					{Term: map[string]types.TermQuery{versionKey: {Value: *latestVersion}}},
					{
						Terms: &types.TermsQuery{
							TermsQuery: map[string]types.TermsQueryField{
								assetIdKey: termFields,
							},
						},
					},
				},
			},
		},
		Size: some.Int(10000),
		Source_: types.SourceFilter{
			Includes: []string{assetIdKey},
		},
	}

	logger.DebugContext(ctx, "Search assets in Elasticsearch", append([]any{slog.String("index", e.assets_index)}, logargs...)...)
	resp, err := e.es.Search().Index(e.assets_index).Request(&query).TrackTotalHits(true).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to search: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType))
		return nil, err
	}
	for _, hit := range resp.Hits.Hits {
		jsonAsset, err := hit.Source_.MarshalJSON()
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal asset: "+err.Error(), slog.String("kbId", kbId))
			return nil, err
		}
		var asset ElasticAssetDocument
		json.Unmarshal(jsonAsset, &asset)
		existsMap[asset.AssetId] = true
	}
	existCount := 0
	notExistCount := 0
	for _, exist := range existsMap {
		if exist {
			existCount++
		} else {
			notExistCount++
		}
	}
	logger.InfoContext(
		ctx, "Successfully checked if assets exist",
		slog.String("kbId", kbId),
		slog.String("assetType", assetType),
		slog.Int("existCount", existCount),
		slog.Int("notExistCount", notExistCount),
	)
	return existsMap, nil
}

func (e *ElasticsearchV8AssetManagerRepo) GetAssetsByKbId(ctx context.Context, kbId string) ([]am.ContextualAsset, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Getting assets by kbId", slog.String("kbId", kbId))
	query := search.Request{
		Query: &types.Query{
			Term: map[string]types.TermQuery{
				kbIdKey: {Value: kbId},
			},
		},
	}
	resp, err := e.es.Search().Index(e.assets_index).Request(&query).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to search: "+err.Error(), slog.String("kbId", kbId))
		return nil, err
	}
	assets := []am.ContextualAsset{}
	for _, hit := range resp.Hits.Hits {
		jsonAsset, err := hit.Source_.MarshalJSON()
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal asset: "+err.Error(), slog.String("kbId", kbId))
			return nil, err
		}
		var asset ElasticAssetDocument
		json.Unmarshal(jsonAsset, &asset)
		assets = append(assets, asset.ContextualAsset)
	}
	logger.InfoContext(ctx, "Successfully retrieved assets by kbId", slog.String("kbId", kbId), slog.Int("assets", len(assets)))
	return assets, nil
}

func (e *ElasticsearchV8AssetManagerRepo) getDocumentId(assetId string, version int) string {
	return fmt.Sprintf("%s:%d", assetId, version)
}

func (e *ElasticsearchV8AssetManagerRepo) InsertAsset(ctx context.Context, asset am.ContextualAsset) (bool, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Inserting asset", slog.Any("asset", asset))
	timestamp := time.Now()
	assetDoc := ElasticAssetDocument{
		ContextualAsset: asset,
		CreatedAt:       timestamp,
		UpdatedAt:       timestamp,
	}
	docId := e.getDocumentId(asset.AssetId, asset.Version)
	logger.DebugContext(ctx, "Insert asset into Elasticsearch", slog.String("index", e.assets_index), slog.Any("asset", asset))
	resp, err := e.es.Index(e.assets_index).Id(docId).Document(assetDoc).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to index asset: "+err.Error(), slog.Any("asset", asset))
		return false, err
	}
	logger.InfoContext(ctx, "Successfully inserted asset", slog.String("docId", resp.Id_), slog.String("index", resp.Index_), slog.Any("result", resp.Result))
	return true, nil
}

func (e *ElasticsearchV8AssetManagerRepo) InsertBatchAssets(ctx context.Context, assets []am.ContextualAsset) (int, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Inserting batch assets", slog.Int("assets", len(assets)))
	timestamp := time.Now()
	bulk := e.es.Bulk()
	for _, asset := range assets {
		bulk.IndexOp(
			types.IndexOperation{
				Index_: some.String(e.assets_index),
				Id_:    some.String(e.getDocumentId(asset.AssetId, asset.Version)),
			},
			ElasticAssetDocument{
				ContextualAsset: asset,
				CreatedAt:       timestamp,
				UpdatedAt:       timestamp,
			},
		)
	}

	logger.DebugContext(ctx, "Bulk insert assets into Elasticsearch", slog.String("index", e.assets_index), slog.Int("assets", len(assets)))
	res, err := bulk.Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to bulk insert assets: "+err.Error())
		return 0, err
	}
	errs := []error{}
	var success int = 0

	if res.Errors {
		logger.Warn("There're some assets failed to insert")
	}
	for _, items := range res.Items {
		for op, item := range items {
			if item.Error != nil {
				id_ := ""
				if item.Id_ != nil {
					id_ = *item.Id_
				}
				reason := "unknown reason"
				if item.Error.Reason != nil {
					reason = *item.Error.Reason
				}
				msg := fmt.Sprintf("Failed to %s asset: %s", op.String(), reason)
				logger.ErrorContext(ctx, msg, slog.String("operation", op.String()), slog.String("id", id_), slog.String("type", item.Error.Type), slog.Int("status", item.Status))
				errs = append(errs, errors.New(msg))
			} else {
				success++
			}
		}
	}

	if len(errs) > 0 {
		msg := fmt.Sprintf("Failed to bulk insert assets with %d errors", len(errs))
		logger.ErrorContext(ctx, msg, slog.Int("errors", len(errs)), slog.Int("success", success), slog.Int("total", len(assets)))
		return success, errors.New(msg)
	}
	logger.InfoContext(ctx, "Successfully bulk inserted assets", slog.Int("success", success), slog.Int("total", len(assets)))
	return success, nil
}

func (e *ElasticsearchV8AssetManagerRepo) AddPainlessScript(script *types.Script, fieldName string, newScript string, newParams map[string]json.RawMessage) error {
	if script.Source == nil {
		script.Source = some.String(newScript)
	} else {
		*script.Source += "\n" + newScript
	}
	if script.Params == nil {
		script.Params = make(map[string]json.RawMessage)
	}
	script.Params[fieldName] = newParams[fieldName]
	return nil
}

func (e *ElasticsearchV8AssetManagerRepo) parseUpdateStringOperation(operation am.UpdateStringOperation, fieldName string, script *types.Script) error {
	switch strings.ToUpper(operation.Operation) {
	case "REPLACE":
		newScript := fmt.Sprintf("ctx._source.%s = params.%s;", fieldName, fieldName)
		value, err := json.Marshal(operation.Value)
		if err != nil {
			return err
		}
		newParams := map[string]json.RawMessage{fieldName: value}
		err = e.AddPainlessScript(script, fieldName, newScript, newParams)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported operation: %s", operation.Operation)
	}
	return nil
}
func (e *ElasticsearchV8AssetManagerRepo) parseUpdateListOperation(operation am.UpdateListOperation, fieldName string, script *types.Script) error {
	switch strings.ToUpper(operation.Operation) {
	case "REPLACE":
		newScript := fmt.Sprintf("ctx._source.%s = params.%s;", fieldName, fieldName)
		value, err := json.Marshal(operation.Values)
		if err != nil {
			return err
		}
		newParams := map[string]json.RawMessage{fieldName: value}
		err = e.AddPainlessScript(script, fieldName, newScript, newParams)
		if err != nil {
			return err
		}
	case "INSERT":
		if operation.Index == nil {
			newScript := fmt.Sprintf("ctx._source.%s.addAll(params.%s);", fieldName, fieldName)
			value, err := json.Marshal(operation.Values)
			if err != nil {
				return err
			}
			newParams := map[string]json.RawMessage{fieldName: value}
			err = e.AddPainlessScript(script, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		} else {
			newScript := fmt.Sprintf("ctx._source.%s.add(params.%s.index, params.%s.value);", fieldName, fieldName, fieldName)
			value, err := json.Marshal(map[string]any{"index": *operation.Index, "value": operation.Values})
			if err != nil {
				return err
			}
			newParams := map[string]json.RawMessage{fieldName: value}
			err = e.AddPainlessScript(script, fieldName, newScript, newParams)
			if err != nil {
				return err
			}
		}
	case "REMOVE":
		newScript := fmt.Sprintf("ctx._source.%s.removeAll(params.%s);", fieldName, fieldName)
		value, err := json.Marshal(operation.Values)
		if err != nil {
			return err
		}
		newParams := map[string]json.RawMessage{fieldName: value}
		err = e.AddPainlessScript(script, fieldName, newScript, newParams)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported operation: %s", operation.Operation)
	}
	return nil
}

func (e *ElasticsearchV8AssetManagerRepo) parseUpdateMapOperation(operation am.UpdateMapOperation, fieldName string, script *types.Script) error {
	switch strings.ToUpper(operation.Operation) {
	case "REPLACE":
		newScript := fmt.Sprintf("ctx._source.%s = params.%s;", fieldName, fieldName)
		value, err := json.Marshal(operation.Values)
		if err != nil {
			return err
		}
		newParams := map[string]json.RawMessage{fieldName: value}
		err = e.AddPainlessScript(script, fieldName, newScript, newParams)
		if err != nil {
			return err
		}
	case "MERGE":
		newScript := fmt.Sprintf("ctx._source.%s.putAll(params.%s);", fieldName, fieldName)
		value, err := json.Marshal(operation.Values)
		if err != nil {
			return err
		}
		newParams := map[string]json.RawMessage{fieldName: value}
		err = e.AddPainlessScript(script, fieldName, newScript, newParams)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported operation: %s", operation.Operation)
	}
	return nil
}

func (e *ElasticsearchV8AssetManagerRepo) UpdateAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int, updatedAsset am.UpdatedContextualAsset) (*am.ContextualAsset, error) {
	logger := am.GetLogger(e)
	logargs := []any{
		slog.String("kbId", kbId),
		slog.String("assetType", assetType),
		slog.String("assetId", assetId),
		slog.Any("updateOps", updatedAsset),
	}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	logger.InfoContext(ctx, "Updating asset", logargs...)
	if kbId == "" || assetType == "" || assetId == "" {
		logger.ErrorContext(ctx, "Missing required fields", slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId))
		return nil, fmt.Errorf("missing required fields, kbId=%s, assetType=%s, assetId=%s", kbId, assetType, assetId)
	}
	if version == nil {
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId), slog.Int("version", 0)) // 0 as placeholder
			return nil, err
		}
		if latestVersion == nil {
			logger.ErrorContext(ctx, "No version provided and no existing asset found", slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId))
			return nil, fmt.Errorf("no version provided and no existing asset found, kbId=%s, assetType=%s, assetId=%s", kbId, assetType, assetId)
		}
		version = latestVersion
	}

	sourceScript := ""
	params := map[string]json.RawMessage{}
	updateScript := types.Script{
		Lang:   &scriptlanguage.Painless,
		Source: &sourceScript,
		Params: params,
	}
	timestamp := time.Now()

	updateAt, err := json.Marshal(timestamp)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to marshal timestamp: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId), slog.Int("version", *version))
		return nil, err
	}
	e.AddPainlessScript(
		&updateScript,
		updatedAtKey,
		fmt.Sprintf("ctx._source.%s = params.%s;", updatedAtKey, updatedAtKey),
		map[string]json.RawMessage{
			updatedAtKey: json.RawMessage(updateAt),
		},
	)
	if updatedAsset.Content != nil {
		e.parseUpdateStringOperation(*updatedAsset.Content, contentKey, &updateScript)
	}
	if updatedAsset.EmbeddingModel != nil {
		e.parseUpdateStringOperation(*updatedAsset.EmbeddingModel, embeddingModelKey, &updateScript)
	}
	if updatedAsset.Refs != nil {
		e.parseUpdateListOperation(*updatedAsset.Refs, refsKey, &updateScript)
	}
	if updatedAsset.Labels != nil {
		e.parseUpdateListOperation(*updatedAsset.Labels, labelsKey, &updateScript)
	}
	if updatedAsset.Metadata != nil {
		e.parseUpdateMapOperation(*updatedAsset.Metadata, metadataKey, &updateScript)
	}
	docId := e.getDocumentId(assetId, *version)
	logger.DebugContext(
		ctx,
		"Update asset in Elasticsearch",
		append(
			[]any{slog.String("index", e.assets_index)},
			logargs...,
		)...,
	)
	_, err = e.es.Update(e.assets_index, docId).Script(&updateScript).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to update asset: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId), slog.Int("version", *version))
		return nil, err
	}
	assets, err := e.GetAssets(ctx, kbId, assetType, &[]string{assetId}, version, nil)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get updated asset: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId), slog.Int("version", *version))
		return nil, err
	}
	if len(assets) == 0 {
		logger.ErrorContext(ctx, "Asset not found", slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId), slog.Int("version", *version))
		return nil, fmt.Errorf("asset not found")
	}
	logger.InfoContext(ctx, "Asset updated successfully", slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId), slog.Int("version", *version))
	return &assets[0], nil
}

func (e *ElasticsearchV8AssetManagerRepo) DeleteAsset(ctx context.Context, kbId string, assetType string, assetId string, version *int) (bool, error) {
	logger := am.GetLogger(e)
	logargs := []any{
		slog.String("kbId", kbId),
		slog.String("assetType", assetType),
		slog.String("assetId", assetId),
	}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	logger.InfoContext(ctx, "Deleting asset", logargs...)
	if version == nil {
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions: "+err.Error(), slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId))
			return false, err
		}
		if latestVersion == nil {
			logger.InfoContext(ctx, "No version provided and no existing asset found", slog.String("kbId", kbId), slog.String("assetType", assetType), slog.String("assetId", assetId))
			return false, nil // Nothing to delete
		}
		version = latestVersion
	}
	docId := e.getDocumentId(assetId, *version)

	logger.DebugContext(
		ctx,
		"Delete asset from Elasticsearch",
		append(
			[]any{slog.String("index", e.assets_index)},
			logargs...,
		)...,
	)
	resp, err := e.es.Delete(e.assets_index, docId).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete asset: "+err.Error(), logargs...)
		return false, err
	}
	if resp.Result.Name == result.Deleted.Name {
		logger.InfoContext(ctx, "Asset deleted successfully", append(logargs, slog.String("result", resp.Result.Name))...)
		return true, nil
	} else if resp.Result.Name == result.Notfound.Name {
		logger.InfoContext(ctx, "Asset not found", append(logargs, slog.String("result", resp.Result.Name))...)
		return false, nil // Already deleted
	} else {
		logger.ErrorContext(ctx, "Failed to delete asset because: result is not deleted", append(logargs, slog.String("result", resp.Result.Name))...)
		return false, fmt.Errorf("failed to delete asset because: result is not deleted, got %s", resp.Result.Name)
	}
}

func (e *ElasticsearchV8AssetManagerRepo) DeleteAssetsByKbId(ctx context.Context, kbId string) (int, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Deleting assets by kbId", slog.String("kbId", kbId))
	logger.DebugContext(ctx, "Delete assets from Elasticsearch", slog.String("index", e.assets_index), slog.String("kbId", kbId))
	resp, err := e.es.DeleteByQuery(e.assets_index).Query(
		&types.Query{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
	).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete assets: "+err.Error(), slog.String("kbId", kbId))
		return 0, err
	}
	if len(resp.Failures) > 0 {
		for _, failure := range resp.Failures {
			reason := "unknown reason"
			if failure.Cause.Reason != nil {
				reason = *failure.Cause.Reason
			}
			logger.ErrorContext(ctx, "Failed to delete asset: "+reason, slog.String("kbId", kbId), slog.String("failure", reason))
		}
		return 0, fmt.Errorf("failed to delete assets because: there are some failures.")
	}
	if resp.Deleted == nil {
		logger.ErrorContext(ctx, "Failed to delete assets because: deleted signal is nil", slog.String("kbId", kbId))
		return 0, fmt.Errorf("failed to delete assets because: deleted is nil")
	}
	deleted := int(*resp.Deleted)
	logger.InfoContext(ctx, "Assets deleted successfully", slog.String("kbId", kbId), slog.Int("deleted", deleted))
	return deleted, nil
}

func (e *ElasticsearchV8AssetManagerRepo) DeleteAssetsByKbIdAndVersion(ctx context.Context, kbId string, version *int) (int, error) {
	logger := am.GetLogger(e)
	logargs := []any{slog.String("kbId", kbId)}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	logger.InfoContext(ctx, "Deleting assets by kbId", logargs...)
	if version == nil {
		latestVersion, err := e.getLatestVersions(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions: "+err.Error(), slog.String("kbId", kbId))
			return 0, err
		}
		if latestVersion == nil {
			logger.InfoContext(ctx, "No version provided and no existing asset found", slog.String("kbId", kbId))
			return 0, nil // Nothing to delete
		}
		version = latestVersion
	}

	logger.DebugContext(ctx, "Delete assets from Elasticsearch", append([]any{slog.String("index", e.assets_index)}, logargs...)...)
	resp, err := e.es.DeleteByQuery(e.assets_index).Query(
		&types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
					{Term: map[string]types.TermQuery{versionKey: {Value: *version}}},
				},
			},
		},
	).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete assets: "+err.Error(), logargs...)
		return 0, err
	}
	if len(resp.Failures) > 0 {
		for _, failure := range resp.Failures {
			reason := "unknown reason"
			if failure.Cause.Reason != nil {
				reason = *failure.Cause.Reason
			}
			logger.ErrorContext(ctx, "Failed to delete asset: "+reason, slog.String("kbId", kbId), slog.String("failure", reason))
		}
		return 0, fmt.Errorf("failed to delete assets because: there are some failures.")
	}
	if resp.Deleted == nil {
		logger.ErrorContext(ctx, "Failed to delete assets because: deleted signal is nil", slog.String("kbId", kbId))
		return 0, fmt.Errorf("failed to delete assets because: deleted is nil")
	}
	deleted := int(*resp.Deleted)
	logger.InfoContext(ctx, "Assets deleted successfully", slog.String("kbId", kbId), slog.Int("deleted", deleted))
	return deleted, nil
}

func (e *ElasticsearchV8AssetManagerRepo) DeleteAssetsByKbIdAndAssetType(ctx context.Context, kbId string, assetType string) (int, error) {
	logger := am.GetLogger(e)
	logargs := []any{slog.String("kbId", kbId), slog.String("assetType", assetType)}
	logger.InfoContext(ctx, "Deleting assets by kbId and assetType", logargs...)
	logger.DebugContext(ctx, "Delete assets from Elasticsearch", append([]any{slog.String("index", e.assets_index)}, logargs...)...)
	resp, err := e.es.DeleteByQuery(e.assets_index).Query(
		&types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
					{Term: map[string]types.TermQuery{assetTypeKey: {Value: assetType}}},
				},
			},
		},
	).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete assets: "+err.Error(), logargs...)
		return 0, err
	}
	if len(resp.Failures) > 0 {
		for _, failure := range resp.Failures {
			reason := "unknown reason"
			if failure.Cause.Reason != nil {
				reason = *failure.Cause.Reason
			}
			logger.ErrorContext(ctx, "Failed to delete asset: "+reason, slog.String("kbId", kbId), slog.String("failure", reason))
		}
		return 0, fmt.Errorf("failed to delete assets because: there are some failures.")
	}
	if resp.Deleted == nil {
		logger.ErrorContext(ctx, "Failed to delete assets because: deleted signal is nil", slog.String("kbId", kbId))
		return 0, fmt.Errorf("failed to delete assets because: deleted is nil")
	}
	deleted := int(*resp.Deleted)
	logger.InfoContext(ctx, "Assets deleted successfully", slog.String("kbId", kbId), slog.Int("deleted", deleted))
	return deleted, nil
}

type ElasticsearchV8VectorRepo struct {
	vectors_index string
	es            *elasticsearch.TypedClient
	embedder      am.Embedder
	similarity    string
	am.LoggingCapacity
}

func NewElasticsearchV8VectorRepo(
	vectors_index string,
	es *elasticsearch.TypedClient,
	embedder am.Embedder,
	similarity string,
) *ElasticsearchV8VectorRepo {
	if similarity == "" {
		similarity = "cosine"
	}
	repo := &ElasticsearchV8VectorRepo{
		vectors_index:   vectors_index,
		es:              es,
		similarity:      similarity,
		LoggingCapacity: *am.GetDefaultLoggingCapacity(),
	}
	if embedder != nil {
		repo.SetEmbedder(context.Background(), embedder)
	}
	return repo
}

func (e *ElasticsearchV8VectorRepo) SetEmbedder(ctx context.Context, embedder am.Embedder) error {
	e.embedder = embedder
	return nil
}

func (e *ElasticsearchV8VectorRepo) Setup(ctx context.Context) error {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Setting up ElasticsearchVectorRepo", slog.String("vectors_index", e.vectors_index))
	if e.embedder == nil {
		logger.ErrorContext(ctx, "embedder not set")
		return fmt.Errorf("embedder not set")
	}

	exist, err := e.es.Indices.Exists(e.vectors_index).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to check if index exists: "+err.Error(), slog.String("vectors_index", e.vectors_index))
		return err
	}

	if !exist {
		logger.InfoContext(ctx, "Index not found, creating index", slog.String("vectors_index", e.vectors_index))
		sim := densevectorsimilarity.DenseVectorSimilarity{}
		_ = sim.UnmarshalText([]byte(e.similarity)) // always return nil
		mapping := types.TypeMapping{
			Properties: map[string]types.Property{
				kbIdKey:           types.KeywordProperty{},
				assetIdKey:        types.KeywordProperty{},
				versionKey:        types.LongNumberProperty{},
				contentKey:        types.TextProperty{},
				embeddingModelKey: types.KeywordProperty{},
				embededVectorKey: types.DenseVectorProperty{
					Dims:       some.Int(e.embedder.GetEmbeddingDim()),
					Similarity: &sim,
				},
				refsKey: types.NestedProperty{
					Properties: map[string]types.Property{
						kbIdKey:      types.KeywordProperty{},
						assetTypeKey: types.KeywordProperty{},
						assetIdKey:   types.KeywordProperty{},
						refTypeKey:   types.KeywordProperty{},
					},
				},
				labelsKey:    types.KeywordProperty{},
				metadataKey:  types.ObjectProperty{},
				createdAtKey: types.DateProperty{},
				updatedAtKey: types.DateProperty{},
			},
		}

		logger.Debug("Creating index", slog.String("vectors_index", e.vectors_index))
		resp, err := e.es.Indices.Create(e.vectors_index).Mappings(&mapping).Do(ctx)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to create index: "+err.Error(), slog.String("vectors_index", e.vectors_index))
			return err
		}
		if !resp.Acknowledged {
			logger.WarnContext(
				ctx,
				"Index creation not acknowledged (may be due to request timeout before the Master node could confirm the update), but does not necessarily mean the index creation failed, please ensure the index is healthy",
				slog.String("vectors_index", e.vectors_index),
			)
		}
	}

	// ensure index is healthy
	logger.Debug("Checking index health", slog.String("vectors_index", e.vectors_index))
	healthResp, err := e.es.Cluster.Health().Index(e.vectors_index).WaitForStatus(healthstatus.Yellow).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to wait for index health: "+err.Error(), slog.String("vectors_index", e.vectors_index))
		return err
	}
	logger.InfoContext(ctx, "Elasticsearch Vector Repo setup complete", slog.String("vectors_index", e.vectors_index), slog.String("status", healthResp.Status.String()))
	return nil
}

func (e *ElasticsearchV8VectorRepo) EmbedContent(ctx context.Context, content string) ([]float32, error) {
	logger := am.GetLogger(e)
	truncatedContent := content // For logging only
	if len(content) > 20 {
		truncatedContent = content[:20] + "..."
	}
	logger.InfoContext(ctx, "Embedding content", slog.String("content", truncatedContent))
	if e.embedder == nil {
		logger.ErrorContext(ctx, "embedder not set")
		return nil, fmt.Errorf("embedder not set")
	}
	logger.DebugContext(ctx, "Sending to embedder", slog.String("content", truncatedContent))
	vector, err := e.embedder.Embed(ctx, content)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to embed content: "+err.Error(), slog.String("content", truncatedContent))
		return nil, err
	}
	logger.InfoContext(ctx, "Content embedded successfully", slog.String("content", truncatedContent), slog.String("embeddingModel", e.embedder.GetEmbeddingModel()), slog.Int("vectorDim", len(vector)))
	return vector, nil
}

func (e *ElasticsearchV8VectorRepo) EmbedAsset(ctx context.Context, asset *am.ContextualAsset, contentConstructorFn *func(am.ContextualAsset) string) (am.VectorAsset, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Embedding asset", slog.String("assetId", asset.AssetId))
	if e.embedder == nil {
		logger.ErrorContext(ctx, "embedder not set")
		return am.VectorAsset{}, fmt.Errorf("embedder not set")
	}

	var content string
	if contentConstructorFn != nil {
		content = (*contentConstructorFn)(*asset)
	} else {
		content = asset.Content
	}

	truncatedContent := content // For logging only
	if len(content) > 20 {
		truncatedContent = content[:20] + "..."
	}

	logger.DebugContext(ctx, "Sending to embedder", slog.String("assetId", asset.AssetId), slog.String("content", truncatedContent))
	vector, err := e.embedder.Embed(ctx, content)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to embed content: "+err.Error(), slog.String("assetId", asset.AssetId), slog.String("content", truncatedContent))
		return am.VectorAsset{}, err
	}

	embeddingModel := e.embedder.GetEmbeddingModel()
	parentRef := am.AssetRef{
		KbId:      asset.KbId,
		AssetType: asset.AssetType,
		AssetId:   asset.AssetId,
		RefType:   am.AssetRefTypeParent,
	}
	refs := []am.AssetRef{parentRef}

	logger.InfoContext(ctx, "Asset embedded successfully", slog.String("assetId", asset.AssetId), slog.String("embeddingModel", embeddingModel), slog.Int("vectorDim", len(vector)))
	return am.VectorAsset{
		KbId:           asset.KbId,
		AssetId:        asset.AssetId,
		Version:        asset.Version,
		Content:        content,
		Refs:           &refs,
		Labels:         asset.Labels,
		Metadata:       asset.Metadata,
		EmbeddingModel: &embeddingModel,
		EmbededVector:  vector,
	}, nil
}

func (e *ElasticsearchV8VectorRepo) QueryVectors(ctx context.Context, queryVector []float32, topK *int, threshold *float32, filter *am.VectorQueryFilter) ([]am.RetrievedVector, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Querying vectors", slog.Int("queryVectorDim", len(queryVector)), slog.Any("topK", topK), slog.Any("threshold", threshold), slog.Any("filter", filter))
	if e.embedder == nil {
		logger.ErrorContext(ctx, "embedder not set")
		return nil, fmt.Errorf("embedder not set")
	}
	// Validate queryVector is not nil or empty
	if len(queryVector) == 0 {
		logger.ErrorContext(ctx, "queryVector is nil or empty")
		return nil, fmt.Errorf("queryVector cannot be nil or empty")
	}
	// Validate queryVector doesn't contain NaN or Inf values
	for i, val := range queryVector {
		if math.IsNaN(float64(val)) || math.IsInf(float64(val), 0) {
			logger.ErrorContext(ctx, "queryVector contains invalid value", slog.Int("index", i))
			return nil, fmt.Errorf("queryVector contains invalid value at index %d: %f", i, val)
		}
	}
	k := am.DefaultQueryTopK
	if topK != nil {
		k = *topK
	}
	embeddingModel := e.embedder.GetEmbeddingModel()

	filters := []types.Query{
		{
			Term: map[string]types.TermQuery{
				embeddingModelKey: {Value: embeddingModel, CaseInsensitive: some.Bool(true)},
			},
		},
	}
	if filter != nil {
		if len(filter.KbIdsIn) > 0 {
			filters = append(filters, types.Query{
				Terms: &types.TermsQuery{
					TermsQuery: map[string]types.TermsQueryField{
						kbIdKey: filter.KbIdsIn,
					},
				},
			})
		} else if filter.KbId != nil {
			filters = append(filters, types.Query{
				Term: map[string]types.TermQuery{
					kbIdKey: {Value: *filter.KbId},
				},
			})
		}
		if filter.Version != nil {
			filters = append(filters, types.Query{
				Term: map[string]types.TermQuery{
					versionKey: {Value: *filter.Version},
				},
			})
		}
		if filter.Label != nil {
			filters = append(filters, types.Query{
				Term: map[string]types.TermQuery{
					labelsKey: {Value: *filter.Label},
				},
			})
		}
	}

	req := search.Request{
		Knn: []types.KnnSearch{
			{
				Field:         embededVectorKey,
				QueryVector:   queryVector,
				K:             some.Int(k),
				NumCandidates: some.Int(k * 2),
				Filter:        filters,
			},
		},
		Size: &k,
	}

	logger.DebugContext(ctx, "Querying vectors from Elasticsearch", slog.String("index", e.vectors_index))
	resp, err := e.es.Search().Index(e.vectors_index).Request(&req).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to query vectors: "+err.Error())
		return nil, err
	}
	retrievedVectors := []am.RetrievedVector{}
	for _, hit := range resp.Hits.Hits {
		if hit.Score_ != nil {
			score := float32(*hit.Score_)
			if threshold != nil && score < *threshold {
				continue
			}
			jsonAsset, err := hit.Source_.MarshalJSON()
			if err != nil {
				logger.ErrorContext(ctx, "Failed to marshal vector asset: "+err.Error())
				return []am.RetrievedVector{}, err
			}
			var asset ElasticVectorAssetDocument
			json.Unmarshal(jsonAsset, &asset)
			retrievedVectors = append(retrievedVectors, am.RetrievedVector{
				VectorAsset: asset.VectorAsset,
				Score:       &score,
			})
		}
	}

	logger.InfoContext(ctx, "Querying vectors completed", slog.Int("retrievedVectorsCount", len(retrievedVectors)))
	return retrievedVectors, nil
}

func (e *ElasticsearchV8VectorRepo) GetVersions(ctx context.Context, kbId string) ([]int, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Getting versions for kbId", slog.String("kbId", kbId))

	getVersionsQueryRequest := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{
						Term: map[string]types.TermQuery{
							kbIdKey: {Value: kbId},
						},
					},
				},
			},
		},
		Aggregations: map[string]types.Aggregations{
			"versions": {
				Terms: &types.TermsAggregation{
					Field: some.String(versionKey),
					Size:  some.Int(10000),
					Order: map[string]sortorder.SortOrder{
						"_key": sortorder.Asc,
					},
				},
			},
		},
	}

	logger.DebugContext(ctx, "Search vectors in Elasticsearch", slog.String("index", e.vectors_index), slog.String("kbId", kbId))
	resp, err := e.es.Search().Index(e.vectors_index).Request(&getVersionsQueryRequest).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to search for vector aggregations: "+err.Error(), slog.String("kbId", kbId))
		return []int{}, err
	}
	agg := resp.Aggregations["versions"].(*types.LongTermsAggregate)
	if agg.Buckets == nil {
		logger.WarnContext(ctx, "No versions found for kbId", slog.String("kbId", kbId))
		return []int{}, nil
	}
	buckets, ok := agg.Buckets.([]types.LongTermsBucket)
	if !ok {
		err := fmt.Errorf("failed to get asset aggregations: %v", agg.Buckets)
		logger.ErrorContext(ctx, "Failed to get asset aggregations: "+err.Error(), slog.String("kbId", kbId))
		return []int{}, err
	}
	versions := []int{}
	for _, b := range buckets {
		versions = append(versions, int(b.Key))
	}
	return versions, nil
}

func (e *ElasticsearchV8VectorRepo) getLatestVersion(ctx context.Context, kbId string) (*int, error) {
	logger := am.GetLogger(e)
	logger.Debug("Getting latest versions for kbId", slog.String("kbId", kbId))
	aggQuery := search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{
						Term: map[string]types.TermQuery{
							kbIdKey: {Value: kbId},
						},
					},
				},
			},
		},
		Aggregations: map[string]types.Aggregations{
			"latest_version": {
				Max: &types.MaxAggregation{
					Field: some.String(versionKey),
				},
			},
		},
		Size: some.Int(0),
	}

	logger.DebugContext(ctx, "Search vectors in Elasticsearch", slog.String("index", e.vectors_index), slog.String("kbId", kbId))
	resp, err := e.es.Search().Index(e.vectors_index).Request(&aggQuery).Do(ctx)
	if err != nil {
		logger.Error("Failed to search for vector aggregations: "+err.Error(), slog.String("kbId", kbId))
		return nil, err
	}
	agg := resp.Aggregations["latest_version"].(*types.MaxAggregate)
	if agg.Value == nil {
		err := fmt.Errorf("no latest version found for kbId: %s", kbId)
		logger.ErrorContext(ctx, "No latest version found", slog.String("kbId", kbId))
		return nil, err
	}
	latestVersion := int(*agg.Value)
	logger.Debug("Getting latest versions for kbId complete", slog.String("kbId", kbId), slog.Int("latestVersion", latestVersion))
	return &latestVersion, nil
}

func (e *ElasticsearchV8VectorRepo) getDocumentId(assetId string, version int) string {
	return fmt.Sprintf("%s:%d", assetId, version)
}

func (e *ElasticsearchV8VectorRepo) InsertVector(ctx context.Context, vectorAsset am.VectorAsset) (bool, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Inserting vector asset", slog.Any("vectorAsset", vectorAsset))
	timestamp := time.Now()
	assetDoc := ElasticVectorAssetDocument{
		VectorAsset: vectorAsset,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
	logger.DebugContext(ctx, "Insert vector asset into Elasticsearch", slog.String("index", e.vectors_index), slog.Any("vectorAsset", assetDoc))
	docId := e.getDocumentId(vectorAsset.AssetId, vectorAsset.Version)
	resp, err := e.es.Index(e.vectors_index).Id(docId).Document(assetDoc).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to insert vector: "+err.Error(), slog.String("kbId", vectorAsset.KbId), slog.String("assetId", vectorAsset.AssetId), slog.Int("version", vectorAsset.Version))
		return false, err
	}
	logger.InfoContext(ctx, "Successfully inserted vector asset", slog.String("docId", resp.Id_), slog.String("index", resp.Index_), slog.Any("result", resp.Result))
	return true, nil
}

func (e *ElasticsearchV8VectorRepo) InsertBatchVectors(ctx context.Context, vectorAssets []am.VectorAsset) (int, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Inserting batch vectors", slog.Int("assets", len(vectorAssets)))
	timestamp := time.Now()
	bulk := e.es.Bulk()
	for _, vectorAsset := range vectorAssets {
		bulk.IndexOp(
			types.IndexOperation{
				Index_: some.String(e.vectors_index),
				Id_:    some.String(e.getDocumentId(vectorAsset.AssetId, vectorAsset.Version)),
			},
			ElasticVectorAssetDocument{
				VectorAsset: vectorAsset,
				CreatedAt:   timestamp,
				UpdatedAt:   timestamp,
			},
		)
	}

	logger.DebugContext(ctx, "Bulk insert vector assets into Elasticsearch", slog.String("index", e.vectors_index), slog.Int("assets", len(vectorAssets)))
	res, err := bulk.Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to bulk insert vector assets: "+err.Error())
		return 0, err
	}
	errs := []error{}
	var success int = 0

	if res.Errors {
		logger.Warn("There're some vector assets failed to insert")
	}
	for _, items := range res.Items {
		for op, item := range items {
			if item.Error != nil {
				id_ := ""
				if item.Id_ != nil {
					id_ = *item.Id_
				}
				reason := "unknown reason"
				if item.Error.Reason != nil {
					reason = *item.Error.Reason
				}
				msg := fmt.Sprintf("Failed to %s vector asset: %s", op.String(), reason)
				logger.ErrorContext(ctx, msg, slog.String("operation", op.String()), slog.String("id", id_), slog.String("type", item.Error.Type), slog.Int("status", item.Status))
				errs = append(errs, errors.New(msg))
			} else {
				success++
			}
		}
	}

	if len(errs) > 0 {
		msg := fmt.Sprintf("Failed to bulk insert vector assets with %d errors", len(errs))
		logger.ErrorContext(ctx, msg, slog.Int("errors", len(errs)), slog.Int("success", success), slog.Int("total", len(vectorAssets)))
		return success, errors.New(msg)
	}
	logger.InfoContext(ctx, "Successfully bulk inserted vector assets", slog.Int("success", success), slog.Int("total", len(vectorAssets)))
	return success, nil
}

func (e *ElasticsearchV8VectorRepo) DeleteVector(ctx context.Context, kbId string, assetId string, version *int) (bool, error) {
	logger := am.GetLogger(e)
	logargs := []any{
		slog.String("kbId", kbId),
		slog.String("assetId", assetId),
	}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	logger.InfoContext(ctx, "Deleting vector", logargs...)
	if version == nil {
		latestVersion, err := e.getLatestVersion(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions: "+err.Error(), slog.String("kbId", kbId), slog.String("assetId", assetId))
			return false, err
		}
		if latestVersion == nil {
			logger.InfoContext(ctx, "No version provided and no existing vector found", slog.String("kbId", kbId), slog.String("assetId", assetId))
			return false, nil // Nothing to delete
		}
		version = latestVersion
	}
	docId := e.getDocumentId(assetId, *version)

	logger.DebugContext(
		ctx,
		"Delete vector from Elasticsearch",
		append(
			[]any{slog.String("index", e.vectors_index)},
			logargs...,
		)...,
	)
	resp, err := e.es.Delete(e.vectors_index, docId).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete vector: "+err.Error(), logargs...)
		return false, err
	}
	if resp.Result.Name == result.Deleted.Name {
		logger.InfoContext(ctx, "Vector deleted successfully", append(logargs, slog.String("result", resp.Result.Name))...)
		return true, nil
	} else if resp.Result.Name == result.Notfound.Name {
		logger.InfoContext(ctx, "Vector not found", append(logargs, slog.String("result", resp.Result.Name))...)
		return false, nil // Already deleted
	} else {
		logger.ErrorContext(ctx, "Failed to delete vector because: result is not deleted", append(logargs, slog.String("result", resp.Result.Name))...)
		return false, fmt.Errorf("failed to delete vector because: result is not deleted, got %s", resp.Result.Name)
	}
}

func (e *ElasticsearchV8VectorRepo) DeleteVectorsByKbId(ctx context.Context, kbId string) (int, error) {
	logger := am.GetLogger(e)
	logger.InfoContext(ctx, "Deleting vectors by kbId", slog.String("kbId", kbId))
	logger.DebugContext(ctx, "Delete vectors from Elasticsearch", slog.String("index", e.vectors_index), slog.String("kbId", kbId))
	resp, err := e.es.DeleteByQuery(e.vectors_index).Query(
		&types.Query{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
	).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete vectors: "+err.Error(), slog.String("kbId", kbId))
		return 0, err
	}
	if len(resp.Failures) > 0 {
		for _, failure := range resp.Failures {
			reason := "unknown reason"
			if failure.Cause.Reason != nil {
				reason = *failure.Cause.Reason
			}
			logger.ErrorContext(ctx, "Failed to delete vector: "+reason, slog.String("kbId", kbId), slog.String("failure", reason))
		}
		return 0, fmt.Errorf("failed to delete vectors because: there are some failures.")
	}
	if resp.Deleted == nil {
		logger.ErrorContext(ctx, "Failed to delete vectors because: deleted signal is nil", slog.String("kbId", kbId))
		return 0, fmt.Errorf("failed to delete vectors because: deleted is nil")
	}
	deleted := int(*resp.Deleted)
	logger.InfoContext(ctx, "Vectors deleted successfully", slog.String("kbId", kbId), slog.Int("deleted", deleted))
	return deleted, nil
}

func (e *ElasticsearchV8VectorRepo) DeleteVectorsByKbIdAndVersion(ctx context.Context, kbId string, version *int) (int, error) {
	logger := am.GetLogger(e)
	logargs := []any{slog.String("kbId", kbId)}
	if version != nil {
		logargs = append(logargs, slog.Int("version", *version))
	} else {
		logargs = append(logargs, slog.String("version", "latest"))
	}
	logger.InfoContext(ctx, "Deleting vectors by kbId and version", logargs...)
	if version == nil {
		latestVersion, err := e.getLatestVersion(ctx, kbId)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to get latest versions: "+err.Error(), slog.String("kbId", kbId))
			return 0, err
		}
		if latestVersion == nil {
			logger.InfoContext(ctx, "No version provided and no existing vector found", slog.String("kbId", kbId))
			return 0, nil // Nothing to delete
		}
		version = latestVersion
	}

	logger.DebugContext(ctx, "Delete vectors from Elasticsearch", append([]any{slog.String("index", e.vectors_index)}, logargs...)...)
	resp, err := e.es.DeleteByQuery(e.vectors_index).Query(
		&types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{Term: map[string]types.TermQuery{kbIdKey: {Value: kbId}}},
					{Term: map[string]types.TermQuery{versionKey: {Value: *version}}},
				},
			},
		},
	).Do(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete vectors: "+err.Error(), logargs...)
		return 0, err
	}
	if len(resp.Failures) > 0 {
		for _, failure := range resp.Failures {
			reason := "unknown reason"
			if failure.Cause.Reason != nil {
				reason = *failure.Cause.Reason
			}
			logger.ErrorContext(ctx, "Failed to delete vector: "+reason, slog.String("kbId", kbId), slog.String("failure", reason))
		}
		return 0, fmt.Errorf("failed to delete vectors because: there are some failures.")
	}
	if resp.Deleted == nil {
		logger.ErrorContext(ctx, "Failed to delete vectors because: deleted signal is nil", slog.String("kbId", kbId))
		return 0, fmt.Errorf("failed to delete vectors because: deleted is nil")
	}
	deleted := int(*resp.Deleted)
	logger.InfoContext(ctx, "Vectors deleted successfully", slog.String("kbId", kbId), slog.Int("deleted", deleted))
	return deleted, nil
}
