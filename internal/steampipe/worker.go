// Package steampipe hosts Steampipe plugins as a VGI worker: each plugin named
// at ATTACH becomes a schema of the catalog and each plugin table a table.
//
//	ATTACH 'steampipe' AS sp (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-steampipe-worker',
//	    plugins 'csv', config 'csv { paths = ["/data/*.csv"] }');
//	SELECT * FROM sp.csv.products;
package steampipe

import (
	// Embedded DuckDB expression evaluator: the scan re-applies pushed
	// LIKE / expression filters with it (see filter_eval.go).
	_ "github.com/Query-farm/vgi-go/vgi/exprduckdb"

	"github.com/Query-farm/vgi-go/vgi"
)

func allExamples() []exampleQuery {
	return exampleQueries(scanExamples, pluginsDocs.Examples, pluginTablesDocs.Examples, keyColumnsDocs.Examples)
}

// NewWorker builds the worker. Extra options (logging, storage) are appended
// after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(catalogComment),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": mainSchemaComment}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(PluginSecretType),
		vgi.WithAttachOptions(AttachOptionSpecs...),
		vgi.WithAttachValidator(validateAttach),
		vgi.WithCatalogContentsHandler(catalogContents),
		vgi.WithSchemaContentsHandler(schemaTables),
		vgi.WithAttachTableGetHandler(tableGet),
		vgi.WithAttachScanFunctionGetHandler(scanFunctionGet),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	w.RegisterTable(vgi.AsTableFunction[ScanState](&Scan{}))
	for _, t := range NewInfoTables() {
		w.RegisterCatalogTable("main", t)
	}
	return w
}
