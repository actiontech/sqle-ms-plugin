package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"strings"

	"github.com/actiontech/sqle/sqle/driver"
	adaptor "github.com/actiontech/sqle/sqle/pkg/driver"
	_ "github.com/denisenkom/go-mssqldb"
)

type MssqlDialector struct {}

func (d *MssqlDialector) Dialect(dsn *driver.DSN) (string, string) {
	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(dsn.User, dsn.Password),
		Host:   fmt.Sprintf("%s:%s", dsn.Host, dsn.Port),
	}
	if dsn.DatabaseName != "" {
		query := url.Values{}
		query.Add("database", dsn.DatabaseName)
		u.RawQuery = query.Encode()
	}
	return u.Scheme, u.String()
}

func (d *MssqlDialector) String() string {
	return "SQL Server"
}

func (d *MssqlDialector) ShowDatabaseSQL() string {
	return "select name from sys.databases where name not in ('master','tempdb','model','msdb','distribution')"
}

var version string
var printVersion = flag.Bool("version", false, "Print version & exit")

func main() {
	flag.Parse()

	if *printVersion {
		fmt.Println(version)
		return
	}

	plugin := adaptor.NewAdaptor(&MssqlDialector{})

	ruleDQL1 := &driver.Rule{
		Name:     "ms_dql_1",
		Desc:     "禁止不带where条件的查询",
		Category: "DQL规范",
		Level:    driver.RuleLevelError,
	}
	ruleDQL1Handler := func(ctx context.Context, rule *driver.Rule, sql string) (string, error) {
		lowerSql := strings.ToLower(sql)
		if strings.Contains(lowerSql, "select") &&
			!strings.Contains(lowerSql,"where") {
			return rule.Desc, nil
		}
		return "", nil
	}
	plugin.AddRule(ruleDQL1, ruleDQL1Handler)
	plugin.Serve()
}
