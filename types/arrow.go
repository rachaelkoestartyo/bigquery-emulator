package types

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/goccy/go-zetasqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

func TableToARROW(t *bigqueryv2.Table) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(t.Schema.Fields))
	for _, field := range t.Schema.Fields {
		f, err := TableFieldToARROW(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *f)
	}
	return arrow.NewSchema(fields, nil), nil
}

func TableFieldToARROW(f *bigqueryv2.TableFieldSchema) (*arrow.Field, error) {
	field, err := tableFieldToARROW(f)
	if err != nil {
		return nil, err
	}
	switch Mode(f.Mode) {
	case RepeatedMode:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.ListOfField(*field),
		}, nil
	case RequiredMode:
		return field, nil
	}
	field.Nullable = true
	return field, nil
}

func tableFieldToARROW(f *bigqueryv2.TableFieldSchema) (*arrow.Field, error) {
	switch FieldType(f.Type) {
	case FieldInteger:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Int64}, nil
	case FieldBoolean:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Boolean}, nil
	case FieldFloat:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Float64}, nil
	case FieldString:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	case FieldBytes:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.Binary}, nil
	case FieldDate:
		return &arrow.Field{Name: f.Name, Type: arrow.PrimitiveTypes.Date32}, nil
	case FieldDatetime:
		return &arrow.Field{
			Name: f.Name,
			Type: &arrow.TimestampType{Unit: arrow.Microsecond},
			Metadata: arrow.MetadataFrom(
				map[string]string{
					"ARROW:extension:name": "google:sqlType:datetime",
				},
			),
		}, nil
	case FieldTime:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Time64us}, nil
	case FieldTimestamp:
		return &arrow.Field{Name: f.Name, Type: arrow.FixedWidthTypes.Timestamp_us}, nil
	case FieldJSON:
		return &arrow.Field{
			Name: f.Name,
			Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(
				map[string]string{
					"ARROW:extension:name": "google:sqlType:json",
				},
			),
		}, nil
	case FieldRecord:
		fields := make([]arrow.Field, 0, len(f.Fields))
		for _, field := range f.Fields {
			fieldV, err := TableFieldToARROW(field)
			if err != nil {
				return nil, err
			}
			fields = append(fields, *fieldV)
		}
		return &arrow.Field{Name: f.Name, Type: arrow.StructOf(fields...)}, nil
	case FieldNumeric:
		// NUMERIC is a DECIMAL with precision 38, scale 9
		return &arrow.Field{Name: f.Name, Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}}, nil
	case FieldBignumeric:
		// BIGNUMERIC is a DECIMAL with precision 76, scale 38
		// BigQuery supports 76.76 digits (76 full digits, 77th is partial)
		return &arrow.Field{Name: f.Name, Type: &arrow.Decimal256Type{Precision: 76, Scale: 38}}, nil
	case FieldGeography:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	case FieldInterval:
		return &arrow.Field{Name: f.Name, Type: arrow.BinaryTypes.String}, nil
	}
	return nil, fmt.Errorf("unsupported arrow type %s", f.Type)
}

func AppendValueToARROWBuilder(ptrv *string, builder array.Builder) error {
	if ptrv == nil {
		builder.AppendNull()
		return nil
	}
	v := *ptrv
	switch b := builder.(type) {
	case *array.Int64Builder:
		i64, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return err
		}
		b.Append(i64)
		return nil
	case *array.Float64Builder:
		f64, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
		b.Append(f64)
		return nil
	case *array.BooleanBuilder:
		cond, err := strconv.ParseBool(v)
		if err != nil {
			return err
		}
		b.Append(cond)
		return nil
	case *array.StringBuilder:
		b.Append(v)
		return nil
	case *array.BinaryBuilder:
		// Bytes are stored as base64 in BigQuery JSON API, decode to raw bytes
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return fmt.Errorf("failed to decode base64 bytes: %w", err)
		}
		b.Append(decoded)
		return nil
	case *array.Date32Builder:
		t, err := parseDate(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Date32(int32(t.Sub(time.Unix(0, 0)) / (24 * time.Hour))))
		return nil
	case *array.Time64Builder:
		t, err := parseTime(v)
		if err != nil {
			return err
		}
		b.Append(arrow.Time64(microsecondsSinceMidnight(t)))
		return nil
	case *array.TimestampBuilder:
		// Handle datetime strings
		var t arrow.Timestamp
		if strings.Contains(v, "T") {
			parsed, err := arrow.TimestampFromString(v, arrow.Microsecond)
			if err != nil {
				return err
			}
			t = parsed
		} else {
			parsed, err := zetasqlite.TimeFromTimestampValue(v)
			if err != nil {
				return err
			}
			t = arrow.Timestamp(parsed.UnixMicro())
		}
		b.Append(t)
		return nil
	case *array.Decimal128Builder:
		// NUMERIC type: precision 38, scale 9
		// Parse the string value to a big.Rat, then convert to scaled integer
		rat := new(big.Rat)
		if _, ok := rat.SetString(v); !ok {
			return fmt.Errorf("failed to parse decimal value: %s", v)
		}

		// Scale the value by 10^scale to get the integer representation
		scale := int32(9)
		scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)

		// Multiply the rational by the scale factor
		scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(scaleFactor))

		// Convert to integer (this truncates any remaining fractional part)
		scaledInt := new(big.Int).Div(scaled.Num(), scaled.Denom())

		// Convert to decimal128.Num
		num := decimal128.FromBigInt(scaledInt)
		b.Append(num)
		return nil
	case *array.Decimal256Builder:
		// BIGNUMERIC type: precision 77, scale 38
		// Parse the string value to a big.Rat, then convert to scaled integer
		rat := new(big.Rat)
		if _, ok := rat.SetString(v); !ok {
			return fmt.Errorf("failed to parse decimal value: %s", v)
		}

		// Scale the value by 10^scale to get the integer representation
		scale := int32(38)
		scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)

		// Multiply the rational by the scale factor
		scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(scaleFactor))

		// Convert to integer (this truncates any remaining fractional part)
		scaledInt := new(big.Int).Div(scaled.Num(), scaled.Denom())

		// Convert to decimal256.Num
		num := decimal256.FromBigInt(scaledInt)
		b.Append(num)
		return nil
	}
	return fmt.Errorf("unexpected builder type %T", builder)
}
