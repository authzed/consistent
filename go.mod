module github.com/authzed/spicedb

go 1.16

require (
<<<<<<< HEAD
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78 // indirect
	github.com/Masterminds/squirrel v1.5.0
	github.com/Microsoft/go-winio v0.4.16 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/authzed/authzed-go v0.0.2
	github.com/aws/aws-sdk-go v1.40.16
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/cespare/xxhash v1.1.0
	github.com/cockroachdb/cockroach v20.1.15+incompatible
	github.com/containerd/continuity v0.0.0-20210315143101-93e15499afd5 // indirect
=======
	cloud.google.com/go/spanner v1.30.1
	github.com/Masterminds/squirrel v1.5.2
	github.com/authzed/authzed-go v0.5.1-0.20220303182356-243e79ca06fa
	github.com/authzed/grpcutil v0.0.0-20220104222419-f813f77722e5
	github.com/aws/aws-sdk-go v1.43.31
	github.com/benbjohnson/clock v1.3.0
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cncf/udpa/go v0.0.0-20220112060539-c52dc94e7fbe // indirect
	github.com/cncf/xds/go v0.0.0-20220330162227-eded343319d0 // indirect
	github.com/dalzilio/rudd v1.1.0
	github.com/dave/jennifer v1.5.0 // indirect
>>>>>>> 09ea166b (gomod: bump xxhash to v2)
	github.com/dgraph-io/ristretto v0.1.0
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
<<<<<<< HEAD
	github.com/emirpasic/gods v1.12.0
	github.com/envoyproxy/protoc-gen-validate v0.6.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6
	github.com/gotestyourself/gotestyourself v2.2.0+incompatible // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2 v2.0.0-rc.2
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-memdb v1.3.2
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/johannesboyne/gofakes3 v0.0.0-20210608054100-92d5d4af5fde
	github.com/jwangsadinata/go-multimap v0.0.0-20190620162914-c29f3d7f33b6
	github.com/jzelinskie/cobrautil v0.0.0-20210619202150-2fc07a6abd5e
	github.com/jzelinskie/stringz v0.0.1
	github.com/lib/pq v1.10.2
	github.com/ngrok/sqlmw v0.0.0-20200129213757-d5c93a81bec6
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/opencontainers/runc v0.1.1 // indirect
	github.com/ory/dockertest v3.3.5+incompatible
	github.com/prometheus/client_golang v1.11.0
	github.com/rs/zerolog v1.23.0
	github.com/scylladb/go-set v1.0.2
<<<<<<< HEAD
	github.com/shopspring/decimal v1.2.0
=======
	github.com/sercand/kuberesolver/v3 v3.1.0
	github.com/shopspring/decimal v1.3.1
>>>>>>> 2610d945 (use consistent-hash load balancer with kubernetes endpoint resolver for)
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.22.0
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/exporters/jaeger v1.0.0-RC2
	go.opentelemetry.io/otel/sdk v1.0.0-RC2
	go.opentelemetry.io/otel/trace v1.0.0-RC2
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	google.golang.org/genproto v0.0.0-20210630183607-d20f26d13c79
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
<<<<<<< HEAD
	gotest.tools v2.2.0+incompatible // indirect
=======
	mvdan.cc/gofumpt v0.3.0
>>>>>>> c45995e4 (bump gofumpt to 1.3.0 and fix new formatting issues)
=======
	github.com/envoyproxy/go-control-plane v0.10.2-0.20220325020618-49ff273808a1 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-uuid v1.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/iancoleman/strcase v0.2.0 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.1 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.15.10 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lyft/protoc-gen-star v0.6.1 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/opencontainers/runc v1.1.2 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/shabbyrobe/gocovmerge v0.0.0-20190829150210-3e036491d500 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/spf13/afero v1.9.3 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/viper v1.14.0 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/subosito/gotenv v1.4.1 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	go.buf.build/protocolbuffers/go/gogo/protobuf v1.3.9 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.10.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.10.0 // indirect
	go.opentelemetry.io/otel/metric v0.32.1 // indirect
	go.opentelemetry.io/otel/sdk v1.10.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/crypto v0.0.0-20220926161630-eccd6366d1be // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/oauth2 v0.2.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/time v0.0.0-20220609170525-579cf78fd858 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	k8s.io/apimachinery v0.25.0 // indirect
	k8s.io/client-go v0.25.0 // indirect
	k8s.io/klog/v2 v2.70.1 // indirect
	k8s.io/utils v0.0.0-20220728103510-ee6ede2d64ed // indirect
>>>>>>> 38b7c9d3 (fix linter error and vuln)
)
