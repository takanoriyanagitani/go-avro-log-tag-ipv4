package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	li "github.com/takanoriyanagitani/go-avro-log-tag-ipv4"
	dh "github.com/takanoriyanagitani/go-avro-log-tag-ipv4/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-log-tag-ipv4/avro/enc/hamba"
	. "github.com/takanoriyanagitani/go-avro-log-tag-ipv4/util"
)

func envVarByKey(key string) IO[string] {
	return func(_ context.Context) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	}
}

func filenameToStringLimited(limit int64) func(string) IO[string] {
	return func(filename string) IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()

			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}

			var bldr strings.Builder

			_, e = io.Copy(&bldr, limited)
			return bldr.String(), e
		}
	}
}

const SchemaSizeLimitDefault int64 = 1048576

var schemaName IO[string] = envVarByKey("ENV_SCHEMA_FILENAME")

var schemaContent IO[string] = Bind(
	schemaName,
	filenameToStringLimited(SchemaSizeLimitDefault),
)

var ipSourceColName IO[string] = envVarByKey("ENV_BODY_NAME").
	Or(Of(string(li.IpSourceColumnNameDefault)))

var tagColumnName IO[string] = envVarByKey("ENV_TAG_NAME").
	Or(Of(string(li.TagColumnNameDefault)))

var config IO[li.Config] = Bind(
	All(
		ipSourceColName,
		tagColumnName,
	),
	Lift(func(s []string) (li.Config, error) {
		return li.Config{
			IpSourceColumnName: li.IpSourceColumnName(s[0]),
			TagColumnName:      li.TagColumnName(s[1]),
		}, nil
	}),
)

var addExtractedIp IO[li.AddExtractedIp] = Of(li.AddExtractedIpDefault)

var converter IO[li.Converter] = Bind(
	config,
	func(cfg li.Config) IO[li.Converter] {
		return Bind(
			addExtractedIp,
			Lift(func(a li.AddExtractedIp) (li.Converter, error) {
				return li.Converter{
					Config:         cfg,
					AddExtractedIp: a,
				}, nil
			}),
		)
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.StdinToMapsDefault

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	converter,
	func(conv li.Converter) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			Lift(func(
				original iter.Seq2[map[string]any, error],
			) (iter.Seq2[map[string]any, error], error) {
				return conv.MapsToTagged(original), nil
			}),
		)
	},
)

var stdin2maps2mapd2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2maps2mapd2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
