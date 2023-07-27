package xk6outputclickhouse

import (
	"github.com/Maksimall89/xk6-output-clickhouse/pkg"
	"go.k6.io/k6/output"
)

func init() {
	output.RegisterExtension(pkg.NameOutput, pkg.New)
}

// TODO write readme.md
// TODO make gitlab-ci linter docker, go
