// Mapping Steampipe column types and values to Arrow.
//
//	Steampipe            DuckDB
//	BOOL                 BOOLEAN
//	INT                  BIGINT
//	DOUBLE               DOUBLE
//	STRING               VARCHAR
//	JSON                 JSON
//	TIMESTAMP, DATETIME  TIMESTAMP WITH TIME ZONE
//	INET, IPADDR, CIDR   VARCHAR (the address text, e.g. 10.0.0.1 or 10.0.0.0/16)
//	LTREE, UNKNOWN       VARCHAR
//
// Every column is nullable: plugins return NULL for anything they could not
// hydrate.

package steampipe

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

var (
	timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	jsonType      = mustJSONType()
)

func mustJSONType() arrow.DataType {
	t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		panic(err)
	}
	return t
}

// columnTypeDatetime is the deprecated DATETIME type, which plugins built
// with older SDKs still declare.
const columnTypeDatetime = proto.ColumnType(5)

// arrowType maps a Steampipe column type.
func arrowType(t proto.ColumnType) arrow.DataType {
	switch t {
	case proto.ColumnType_BOOL:
		return arrow.FixedWidthTypes.Boolean
	case proto.ColumnType_INT:
		return arrow.PrimitiveTypes.Int64
	case proto.ColumnType_DOUBLE:
		return arrow.PrimitiveTypes.Float64
	case proto.ColumnType_JSON:
		return jsonType
	case proto.ColumnType_TIMESTAMP, columnTypeDatetime:
		return timestampType
	default:
		return arrow.BinaryTypes.String
	}
}

// sqlTypeName is the DuckDB spelling of a Steampipe column type.
func sqlTypeName(t proto.ColumnType) string {
	switch t {
	case proto.ColumnType_BOOL:
		return "BOOLEAN"
	case proto.ColumnType_INT:
		return "BIGINT"
	case proto.ColumnType_DOUBLE:
		return "DOUBLE"
	case proto.ColumnType_JSON:
		return "JSON"
	case proto.ColumnType_TIMESTAMP, columnTypeDatetime:
		return "TIMESTAMP WITH TIME ZONE"
	default:
		return "VARCHAR"
	}
}

// deprecatedContextColumn is the SDK's legacy duplicate of sp_ctx.
const deprecatedContextColumn = "_ctx"

// visibleColumns are the table's columns without the deprecated _ctx.
func visibleColumns(ts *proto.TableSchema) []*proto.ColumnDefinition {
	out := make([]*proto.ColumnDefinition, 0, len(ts.GetColumns()))
	for _, c := range ts.GetColumns() {
		if c.GetName() != deprecatedContextColumn {
			out = append(out, c)
		}
	}
	return out
}

// tableArrowSchema is the DuckDB table schema of one plugin table.
func tableArrowSchema(ts *proto.TableSchema) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(ts.GetColumns()))
	for _, c := range visibleColumns(ts) {
		comment := c.GetDescription()
		if comment == "" {
			comment = fmt.Sprintf("Steampipe column %s (%s).", c.GetName(), strings.ToLower(c.GetType().String()))
		}
		fields = append(fields, arrow.Field{
			Name: c.GetName(), Type: arrowType(c.GetType()), Nullable: true,
			Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment}),
		})
	}
	return arrow.NewSchema(fields, nil)
}

// columnText renders any Steampipe value as text (for VARCHAR columns, and as
// the fallback when a plugin sends a value of another type).
func columnText(c *proto.Column) (string, bool) {
	switch v := c.GetValue().(type) {
	case *proto.Column_StringValue:
		return v.StringValue, true
	case *proto.Column_IpAddrValue:
		return v.IpAddrValue, true
	case *proto.Column_CidrRangeValue:
		return v.CidrRangeValue, true
	case *proto.Column_LtreeValue:
		return v.LtreeValue, true
	case *proto.Column_IntValue:
		return strconv.FormatInt(v.IntValue, 10), true
	case *proto.Column_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'g', -1, 64), true
	case *proto.Column_BoolValue:
		return strconv.FormatBool(v.BoolValue), true
	case *proto.Column_JsonValue:
		return string(v.JsonValue), true
	case *proto.Column_TimestampValue:
		return v.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano), true
	}
	return "", false
}

// appendColumn appends one plugin value to b, converting it to the column's
// declared type. A value that cannot be represented becomes NULL.
func appendColumn(b array.Builder, c *proto.Column) {
	if c == nil || c.GetValue() == nil {
		b.AppendNull()
		return
	}
	if _, isNull := c.GetValue().(*proto.Column_NullValue); isNull {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.BooleanBuilder:
		switch v := c.GetValue().(type) {
		case *proto.Column_BoolValue:
			bb.Append(v.BoolValue)
		case *proto.Column_IntValue:
			bb.Append(v.IntValue != 0)
		default:
			s, _ := columnText(c)
			if x, err := strconv.ParseBool(s); err == nil {
				bb.Append(x)
			} else {
				bb.AppendNull()
			}
		}
	case *array.Int64Builder:
		switch v := c.GetValue().(type) {
		case *proto.Column_IntValue:
			bb.Append(v.IntValue)
		case *proto.Column_DoubleValue:
			if v.DoubleValue == math.Trunc(v.DoubleValue) && math.Abs(v.DoubleValue) < 9.2e18 {
				bb.Append(int64(v.DoubleValue))
			} else {
				bb.AppendNull()
			}
		default:
			s, _ := columnText(c)
			if x, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
				bb.Append(x)
			} else {
				bb.AppendNull()
			}
		}
	case *array.Float64Builder:
		switch v := c.GetValue().(type) {
		case *proto.Column_DoubleValue:
			bb.Append(v.DoubleValue)
		case *proto.Column_IntValue:
			bb.Append(float64(v.IntValue))
		default:
			s, _ := columnText(c)
			if x, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
				bb.Append(x)
			} else {
				bb.AppendNull()
			}
		}
	case *array.TimestampBuilder:
		switch v := c.GetValue().(type) {
		case *proto.Column_TimestampValue:
			bb.Append(arrow.Timestamp(v.TimestampValue.AsTime().UnixMicro()))
		default:
			s, _ := columnText(c)
			if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s)); err == nil {
				bb.Append(arrow.Timestamp(t.UnixMicro()))
			} else {
				bb.AppendNull()
			}
		}
	case *array.ExtensionBuilder: // JSON
		sb := bb.StorageBuilder().(*array.StringBuilder)
		switch v := c.GetValue().(type) {
		case *proto.Column_JsonValue:
			sb.Append(string(v.JsonValue))
		default:
			s, _ := columnText(c)
			enc, _ := json.Marshal(s)
			sb.Append(string(enc))
		}
	case *array.StringBuilder:
		if s, ok := columnText(c); ok {
			bb.Append(s)
		} else {
			bb.AppendNull()
		}
	default:
		b.AppendNull()
	}
}
