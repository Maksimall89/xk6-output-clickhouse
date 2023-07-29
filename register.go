package xk6outputclickhouse

import (
	"github.com/Maksimall89/xk6-output-clickhouse/pkg"
	"go.k6.io/k6/output"
)

func init() {
	output.RegisterExtension(pkg.NameOutput, pkg.New)
}
