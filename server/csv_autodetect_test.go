package server

import (
	"testing"

	"github.com/goccy/bigquery-emulator/types"
)

func TestIsNullValue(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"", true},
		{"  ", true},
		{"null", true},
		{"NULL", true},
		{"Null", true},
		{" null ", true},
		{"0", false},
		{"false", false},
		{"none", false},
		{"hello", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := isNullValue(tc.input)
			if result != tc.expected {
				t.Errorf("isNullValue(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestIsBool(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"false", true},
		{"TRUE", true},
		{"FALSE", true},
		{"True", true},
		{"yes", true},
		{"no", true},
		{"YES", true},
		{"NO", true},
		{"y", true},
		{"n", true},
		{"Y", true},
		{"N", true},
		{"1", true},
		{"0", true},
		{"", false},
		{"maybe", false},
		{"2", false},
		{"truee", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := isBool(tc.input)
			if result != tc.expected {
				t.Errorf("isBool(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestIsInteger(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"0", true},
		{"1", true},
		{"-1", true},
		{"123456789", true},
		{"-123456789", true},
		{"  42  ", true},
		{"1.5", false},
		{"1e5", false},
		{"abc", false},
		{"", false},
		{"12.0", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := isInteger(tc.input)
			if result != tc.expected {
				t.Errorf("isInteger(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestIsFloat(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"0", true},
		{"1", true},
		{"1.5", true},
		{"-1.5", true},
		{"3.14159", true},
		{"1e5", true},
		{"1.5e-10", true},
		{"  3.14  ", true},
		{"abc", false},
		{"", false},
		{"1.2.3", false},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := isFloat(tc.input)
			if result != tc.expected {
				t.Errorf("isFloat(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestDetectDateFormat(t *testing.T) {
	testCases := []struct {
		name           string
		values         []string
		expectedLayout string
	}{
		{
			name:           "iso_format",
			values:         []string{"2024-01-15", "2024-12-31"},
			expectedLayout: "2006-01-02",
		},
		{
			name:           "uk_format_unambiguous",
			values:         []string{"15/01/2024", "25/12/2024"},
			expectedLayout: "02/01/2006",
		},
		{
			name:           "us_format_unambiguous",
			values:         []string{"01/15/2024", "12/25/2024"},
			expectedLayout: "01/02/2006",
		},
		{
			name:           "uk_dash_format",
			values:         []string{"15-01-2024", "25-12-2024"},
			expectedLayout: "02-01-2006",
		},
		{
			name:           "ambiguous_defaults_to_iso",
			values:         []string{"2024-01-05"},
			expectedLayout: "2006-01-02",
		},
		{
			name:           "ambiguous_slash_defaults_to_uk",
			values:         []string{"01/02/2024"},
			expectedLayout: "02/01/2006", // UK is preferred over US when ambiguous
		},
		{
			name:           "not_a_date",
			values:         []string{"hello", "world"},
			expectedLayout: "",
		},
		{
			name:           "empty_values",
			values:         []string{},
			expectedLayout: "",
		},
		{
			name:           "mixed_valid_invalid",
			values:         []string{"2024-01-15", "not-a-date"},
			expectedLayout: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := detectDateFormat(tc.values)
			if result != tc.expectedLayout {
				t.Errorf("detectDateFormat(%v) = %q, want %q", tc.values, result, tc.expectedLayout)
			}
		})
	}
}

func TestInferFieldType(t *testing.T) {
	testCases := []struct {
		name         string
		rows         [][]string
		colIndex     int
		expectedType string
	}{
		{
			name:         "bool_true_false",
			rows:         [][]string{{"true"}, {"false"}, {"true"}},
			colIndex:     0,
			expectedType: "BOOL",
		},
		{
			name:         "bool_yes_no",
			rows:         [][]string{{"yes"}, {"no"}, {"YES"}},
			colIndex:     0,
			expectedType: "BOOL",
		},
		{
			name:         "integer",
			rows:         [][]string{{"1"}, {"42"}, {"-100"}},
			colIndex:     0,
			expectedType: "INTEGER",
		},
		{
			name:         "float",
			rows:         [][]string{{"1.5"}, {"3.14"}, {"-0.5"}},
			colIndex:     0,
			expectedType: "FLOAT",
		},
		{
			name:         "date_iso",
			rows:         [][]string{{"2024-01-15"}, {"2024-12-31"}},
			colIndex:     0,
			expectedType: "DATE",
		},
		{
			name:         "date_uk",
			rows:         [][]string{{"15/01/2024"}, {"31/12/2024"}},
			colIndex:     0,
			expectedType: "DATE",
		},
		{
			name:         "string",
			rows:         [][]string{{"hello"}, {"world"}},
			colIndex:     0,
			expectedType: "STRING",
		},
		{
			name:         "mixed_to_string",
			rows:         [][]string{{"1"}, {"hello"}},
			colIndex:     0,
			expectedType: "STRING",
		},
		{
			name:         "all_null",
			rows:         [][]string{{""}, {"null"}, {"NULL"}},
			colIndex:     0,
			expectedType: "STRING",
		},
		{
			name:         "integer_with_nulls",
			rows:         [][]string{{"1"}, {""}, {"3"}},
			colIndex:     0,
			expectedType: "INTEGER",
		},
		{
			name:         "second_column",
			rows:         [][]string{{"a", "1"}, {"b", "2"}, {"c", "3"}},
			colIndex:     1,
			expectedType: "INTEGER",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := inferFieldType(tc.rows, tc.colIndex)
			if result != tc.expectedType {
				t.Errorf("inferFieldType(%v, %d) = %q, want %q", tc.rows, tc.colIndex, result, tc.expectedType)
			}
		})
	}
}

func TestSelectSampleRows(t *testing.T) {
	testCases := []struct {
		name        string
		rows        [][]string
		maxSamples  int
		expectedLen int
	}{
		{
			name:        "fewer_than_max",
			rows:        [][]string{{"a"}, {"b"}, {"c"}},
			maxSamples:  10,
			expectedLen: 3,
		},
		{
			name:        "exactly_max",
			rows:        [][]string{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}},
			maxSamples:  5,
			expectedLen: 5,
		},
		{
			name:        "more_than_max",
			rows:        [][]string{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"f"}, {"g"}, {"h"}, {"i"}, {"j"}, {"k"}, {"l"}},
			maxSamples:  5,
			expectedLen: 5,
		},
		{
			name:        "empty",
			rows:        [][]string{},
			maxSamples:  10,
			expectedLen: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := selectSampleRows(tc.rows, tc.maxSamples)
			if len(result) != tc.expectedLen {
				t.Errorf("selectSampleRows() returned %d rows, want %d", len(result), tc.expectedLen)
			}
			// Verify first and last rows are included if we have more than maxSamples
			if len(tc.rows) > tc.maxSamples && len(result) > 0 {
				if result[0][0] != tc.rows[0][0] {
					t.Errorf("first row not included in sample")
				}
				if result[len(result)-1][0] != tc.rows[len(tc.rows)-1][0] {
					t.Errorf("last row not included in sample")
				}
			}
		})
	}
}

func TestConvertCSVValue(t *testing.T) {
	testCases := []struct {
		name     string
		value    string
		colType  types.Type
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "integer",
			value:    "42",
			colType:  types.INT64,
			expected: int64(42),
			wantErr:  false,
		},
		{
			name:     "negative_integer",
			value:    "-123",
			colType:  types.INT64,
			expected: int64(-123),
			wantErr:  false,
		},
		{
			name:     "float",
			value:    "3.14",
			colType:  types.FLOAT64,
			expected: 3.14,
			wantErr:  false,
		},
		{
			name:     "bool_true",
			value:    "true",
			colType:  types.BOOL,
			expected: true,
			wantErr:  false,
		},
		{
			name:     "bool_false",
			value:    "false",
			colType:  types.BOOL,
			expected: false,
			wantErr:  false,
		},
		{
			name:     "bool_yes",
			value:    "yes",
			colType:  types.BOOL,
			expected: true,
			wantErr:  false,
		},
		{
			name:     "bool_no",
			value:    "no",
			colType:  types.BOOL,
			expected: false,
			wantErr:  false,
		},
		{
			name:     "bool_y",
			value:    "Y",
			colType:  types.BOOL,
			expected: true,
			wantErr:  false,
		},
		{
			name:     "bool_n",
			value:    "N",
			colType:  types.BOOL,
			expected: false,
			wantErr:  false,
		},
		{
			name:     "date_iso",
			value:    "2024-01-15",
			colType:  types.DATE,
			expected: "2024-01-15",
			wantErr:  false,
		},
		{
			name:     "date_uk_to_iso",
			value:    "15/01/2024",
			colType:  types.DATE,
			expected: "2024-01-15",
			wantErr:  false,
		},
		{
			name:     "date_us_to_iso",
			value:    "01/15/2024",
			colType:  types.DATE,
			expected: "2024-01-15",
			wantErr:  false,
		},
		{
			name:     "string",
			value:    "hello world",
			colType:  types.STRING,
			expected: "hello world",
			wantErr:  false,
		},
		{
			name:     "null_empty",
			value:    "",
			colType:  types.INT64,
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "null_literal",
			value:    "null",
			colType:  types.STRING,
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "null_literal_uppercase",
			value:    "NULL",
			colType:  types.INT64,
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "invalid_integer",
			value:    "not_a_number",
			colType:  types.INT64,
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "invalid_bool",
			value:    "maybe",
			colType:  types.BOOL,
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := convertCSVValue(tc.value, tc.colType)
			if tc.wantErr {
				if err == nil {
					t.Errorf("convertCSVValue(%q, %v) expected error but got none", tc.value, tc.colType)
				}
				return
			}
			if err != nil {
				t.Errorf("convertCSVValue(%q, %v) unexpected error: %v", tc.value, tc.colType, err)
				return
			}
			if result != tc.expected {
				t.Errorf("convertCSVValue(%q, %v) = %v (%T), want %v (%T)", tc.value, tc.colType, result, result, tc.expected, tc.expected)
			}
		})
	}
}

func TestParseBoolValue(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
		wantErr  bool
	}{
		{"true", true, false},
		{"TRUE", true, false},
		{"True", true, false},
		{"false", false, false},
		{"FALSE", false, false},
		{"False", false, false},
		{"yes", true, false},
		{"YES", true, false},
		{"no", false, false},
		{"NO", false, false},
		{"y", true, false},
		{"Y", true, false},
		{"n", false, false},
		{"N", false, false},
		{"1", true, false},
		{"0", false, false},
		{"maybe", false, true},
		{"", false, true},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := parseBoolValue(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseBoolValue(%q) expected error but got none", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseBoolValue(%q) unexpected error: %v", tc.input, err)
				return
			}
			if result != tc.expected {
				t.Errorf("parseBoolValue(%q) = %v, want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestParseAndConvertDate(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
		wantErr  bool
	}{
		{"2024-01-15", "2024-01-15", false},
		{"15/01/2024", "2024-01-15", false},
		{"01/15/2024", "2024-01-15", false},
		{"15-01-2024", "2024-01-15", false},
		{"01-15-2024", "2024-01-15", false},
		{"invalid", "", true},
		{"2024/01/15", "", true}, // Not a supported format
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := parseAndConvertDate(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseAndConvertDate(%q) expected error but got none", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseAndConvertDate(%q) unexpected error: %v", tc.input, err)
				return
			}
			if result != tc.expected {
				t.Errorf("parseAndConvertDate(%q) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}

func TestDetectSchema(t *testing.T) {
	h := &uploadContentHandler{}

	testCases := []struct {
		name            string
		records         [][]string
		skipLeadingRows int64
		expectedFields  map[string]string // column name -> type
		wantErr         bool
	}{
		{
			name: "basic_types",
			records: [][]string{
				{"name", "age", "score", "active"},
				{"Alice", "30", "95.5", "true"},
				{"Bob", "25", "88.0", "false"},
			},
			skipLeadingRows: 0,
			expectedFields: map[string]string{
				"name":   "STRING",
				"age":    "INTEGER",
				"score":  "FLOAT",
				"active": "BOOL",
			},
			wantErr: false,
		},
		{
			name: "with_dates",
			records: [][]string{
				{"event", "date"},
				{"Birthday", "2024-01-15"},
				{"Anniversary", "2024-12-31"},
			},
			skipLeadingRows: 0,
			expectedFields: map[string]string{
				"event": "STRING",
				"date":  "DATE",
			},
			wantErr: false,
		},
		{
			name: "with_nulls",
			records: [][]string{
				{"id", "value"},
				{"1", "100"},
				{"2", ""},
				{"3", "null"},
				{"4", "200"},
			},
			skipLeadingRows: 0,
			expectedFields: map[string]string{
				"id":    "INTEGER",
				"value": "INTEGER",
			},
			wantErr: false,
		},
		{
			name: "all_null_column",
			records: [][]string{
				{"id", "empty"},
				{"1", ""},
				{"2", "null"},
			},
			skipLeadingRows: 0,
			expectedFields: map[string]string{
				"id":    "INTEGER",
				"empty": "STRING",
			},
			wantErr: false,
		},
		{
			name: "header_only",
			records: [][]string{
				{"col1", "col2"},
			},
			skipLeadingRows: 0,
			expectedFields: map[string]string{
				"col1": "STRING",
				"col2": "STRING",
			},
			wantErr: false,
		},
		{
			name:            "empty_csv",
			records:         [][]string{},
			skipLeadingRows: 0,
			expectedFields:  nil,
			wantErr:         true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := h.detectSchema(tc.records, tc.skipLeadingRows)
			if tc.wantErr {
				if err == nil {
					t.Errorf("detectSchema() expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("detectSchema() unexpected error: %v", err)
				return
			}

			if len(schema.Fields) != len(tc.expectedFields) {
				t.Errorf("detectSchema() returned %d fields, want %d", len(schema.Fields), len(tc.expectedFields))
				return
			}

			for _, field := range schema.Fields {
				expectedType, ok := tc.expectedFields[field.Name]
				if !ok {
					t.Errorf("unexpected field %q in schema", field.Name)
					continue
				}
				if field.Type != expectedType {
					t.Errorf("field %q has type %q, want %q", field.Name, field.Type, expectedType)
				}
				if field.Mode != "NULLABLE" {
					t.Errorf("field %q has mode %q, want NULLABLE", field.Name, field.Mode)
				}
			}
		})
	}
}
