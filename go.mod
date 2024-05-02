module github.com/mutility/parquetry

go 1.22.1

require (
	github.com/alecthomas/kong v0.9.0
	github.com/alecthomas/participle/v2 v2.1.1
	github.com/flexera/tabular v0.0.0-20240313132131-df322a4c6a80
	github.com/parquet-go/parquet-go v0.20.1
	github.com/rogpeppe/go-internal v1.12.0
)

require (
	github.com/andybalholm/brotli v1.0.5 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/segmentio/encoding v0.3.6 // indirect
	golang.org/x/sys v0.10.0 // indirect
	golang.org/x/tools v0.1.12 // indirect
)

replace github.com/parquet-go/parquet-go => ../../../flexera/parquet-go

replace github.com/flexera/tabular => ../../../flexera/tabular
