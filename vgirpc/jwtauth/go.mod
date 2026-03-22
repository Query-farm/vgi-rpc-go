module github.com/Query-farm/vgi-rpc/vgirpc/jwtauth

go 1.25.0

require (
	github.com/MicahParks/keyfunc/v3 v3.3.5
	github.com/Query-farm/vgi-rpc v0.0.0
	github.com/golang-jwt/jwt/v5 v5.2.1
)

require (
	github.com/MicahParks/jwkset v0.5.19 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/apache/arrow-go/v18 v18.2.0 // indirect
	github.com/apache/thrift v0.22.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/telemetry v0.0.0-20260209163413-e7419c687ee4 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.42.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)

replace (
	github.com/Query-farm/vgi-rpc => ../..
	github.com/apache/arrow-go/v18 => github.com/rustyconover/arrow-go/v18 v18.0.0-20260220022719-2d45cbd918a4
)
