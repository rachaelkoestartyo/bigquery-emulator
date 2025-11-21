package types

import (
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestAppendValueToARROWBuilder_NumericExtremeValues(t *testing.T) {
	testCases := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "min_numeric",
			value:    "-99999999999999999999999999999.999999999",
			expected: "-99999999999999999999999999999.999999999",
		},
		{
			name:     "max_numeric",
			value:    "99999999999999999999999999999.999999999",
			expected: "99999999999999999999999999999.999999999",
		},
		{
			name:     "zero",
			value:    "0",
			expected: "0.000000000",
		},
		{
			name:     "small_positive",
			value:    "123.456789000",
			expected: "123.456789000",
		},
		{
			name:     "small_negative",
			value:    "-123.456789000",
			expected: "-123.456789000",
		},
	}

	pool := memory.NewGoAllocator()
	decimalType := &arrow.Decimal128Type{Precision: 38, Scale: 9}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewDecimal128Builder(pool, decimalType)
			defer builder.Release()

			// Append the value
			err := AppendValueToARROWBuilder(&tc.value, builder)
			if err != nil {
				t.Fatalf("AppendValueToARROWBuilder failed: %v", err)
			}

			// Build the array
			arr := builder.NewArray()
			defer arr.Release()

			decimalArr := arr.(*array.Decimal128)
			if decimalArr.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", decimalArr.Len())
			}

			// Get the decimal value and convert to string
			decimalValue := decimalArr.Value(0)
			resultStr := decimalValue.ToString(9) // scale = 9

			// Parse both as big.Rat to compare (to handle trailing zeros)
			expectedRat := new(big.Rat)
			if _, ok := expectedRat.SetString(tc.expected); !ok {
				t.Fatalf("failed to parse expected value: %s", tc.expected)
			}

			resultRat := new(big.Rat)
			if _, ok := resultRat.SetString(resultStr); !ok {
				t.Fatalf("failed to parse result value: %s", resultStr)
			}

			if expectedRat.Cmp(resultRat) != 0 {
				t.Errorf("value mismatch:\n  expected: %s\n  got:      %s", tc.expected, resultStr)
			}
		})
	}
}

func TestAppendValueToARROWBuilder_BignumericExtremeValues(t *testing.T) {
	testCases := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "min_bignumeric",
			value:    "-578960446186580977117854925043439539266.34992332820282019728792003956564819968",
			expected: "-578960446186580977117854925043439539266.34992332820282019728792003956564819968",
		},
		{
			name:     "max_bignumeric",
			value:    "578960446186580977117854925043439539266.34992332820282019728792003956564819967",
			expected: "578960446186580977117854925043439539266.34992332820282019728792003956564819967",
		},
		{
			name:     "zero",
			value:    "0",
			expected: "0.00000000000000000000000000000000000000",
		},
		{
			name:     "small_positive",
			value:    "123.456",
			expected: "123.45600000000000000000000000000000000000",
		},
		{
			name:     "small_negative",
			value:    "-123.456",
			expected: "-123.45600000000000000000000000000000000000",
		},
		{
			name:     "halfway_to_max",
			value:    "289480223093290488558927462521719769633.17496166410141009864396001978282409984",
			expected: "289480223093290488558927462521719769633.17496166410141009864396001978282409984",
		},
	}

	pool := memory.NewGoAllocator()
	decimalType := &arrow.Decimal256Type{Precision: 76, Scale: 38}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewDecimal256Builder(pool, decimalType)
			defer builder.Release()

			// Append the value
			err := AppendValueToARROWBuilder(&tc.value, builder)
			if err != nil {
				t.Fatalf("AppendValueToARROWBuilder failed: %v", err)
			}

			// Build the array
			arr := builder.NewArray()
			defer arr.Release()

			decimalArr := arr.(*array.Decimal256)
			if decimalArr.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", decimalArr.Len())
			}

			// Get the decimal value and convert to string
			decimalValue := decimalArr.Value(0)
			resultStr := decimalValue.ToString(38) // scale = 38

			// Parse both as big.Rat to compare
			expectedRat := new(big.Rat)
			if _, ok := expectedRat.SetString(tc.expected); !ok {
				t.Fatalf("failed to parse expected value: %s", tc.expected)
			}

			resultRat := new(big.Rat)
			if _, ok := resultRat.SetString(resultStr); !ok {
				t.Fatalf("failed to parse result value: %s", resultStr)
			}

			if expectedRat.Cmp(resultRat) != 0 {
				t.Errorf("value mismatch:\n  expected: %s\n  got:      %s", tc.expected, resultStr)
			}
		})
	}
}

func TestAppendValueToARROWBuilder_NullValues(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("null_numeric", func(t *testing.T) {
		decimalType := &arrow.Decimal128Type{Precision: 38, Scale: 9}
		builder := array.NewDecimal128Builder(pool, decimalType)
		defer builder.Release()

		err := AppendValueToARROWBuilder(nil, builder)
		if err != nil {
			t.Fatalf("AppendValueToARROWBuilder failed: %v", err)
		}

		arr := builder.NewArray()
		defer arr.Release()

		decimalArr := arr.(*array.Decimal128)
		if decimalArr.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", decimalArr.Len())
		}

		if !decimalArr.IsNull(0) {
			t.Errorf("expected null value, got non-null")
		}
	})

	t.Run("null_bignumeric", func(t *testing.T) {
		decimalType := &arrow.Decimal256Type{Precision: 76, Scale: 38}
		builder := array.NewDecimal256Builder(pool, decimalType)
		defer builder.Release()

		err := AppendValueToARROWBuilder(nil, builder)
		if err != nil {
			t.Fatalf("AppendValueToARROWBuilder failed: %v", err)
		}

		arr := builder.NewArray()
		defer arr.Release()

		decimalArr := arr.(*array.Decimal256)
		if decimalArr.Len() != 1 {
			t.Fatalf("expected 1 value, got %d", decimalArr.Len())
		}

		if !decimalArr.IsNull(0) {
			t.Errorf("expected null value, got non-null")
		}
	})
}

// TestAppendValueToARROWBuilder_NumericRoundTrip verifies end-to-end encoding/decoding
func TestAppendValueToARROWBuilder_NumericRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		value string
	}{
		{"positive", "12345678901234567890123456789.123456789"},
		{"negative", "-12345678901234567890123456789.123456789"},
		{"max", "99999999999999999999999999999.999999999"},
		{"min", "-99999999999999999999999999999.999999999"},
		{"one", "1.0"},
		{"negative_one", "-1.0"},
	}

	pool := memory.NewGoAllocator()
	decimalType := &arrow.Decimal128Type{Precision: 38, Scale: 9}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewDecimal128Builder(pool, decimalType)
			defer builder.Release()

			// Append the value using AppendValueToARROWBuilder
			err := AppendValueToARROWBuilder(&tc.value, builder)
			if err != nil {
				t.Fatalf("AppendValueToARROWBuilder failed: %v", err)
			}

			// Build the array
			arr := builder.NewArray()
			defer arr.Release()

			decimalArr := arr.(*array.Decimal128)
			if decimalArr.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", decimalArr.Len())
			}

			// Get the decimal value and convert to string
			decimalValue := decimalArr.Value(0)
			resultStr := decimalValue.ToString(9) // scale = 9

			// Parse both as big.Rat to compare
			expectedRat := new(big.Rat)
			if _, ok := expectedRat.SetString(tc.value); !ok {
				t.Fatalf("failed to parse expected value: %s", tc.value)
			}

			resultRat := new(big.Rat)
			if _, ok := resultRat.SetString(resultStr); !ok {
				t.Fatalf("failed to parse result value: %s", resultStr)
			}

			if expectedRat.Cmp(resultRat) != 0 {
				t.Errorf("round-trip mismatch:\n  input:    %s\n  output:   %s", tc.value, resultStr)
			}
		})
	}
}

// TestAppendValueToARROWBuilder_BignumericRoundTrip verifies end-to-end encoding/decoding
func TestAppendValueToARROWBuilder_BignumericRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		value string
	}{
		{"positive", "123456789012345678901234567890.12345678901234567890123456789012345678"},
		{"negative", "-123456789012345678901234567890.12345678901234567890123456789012345678"},
		{"max", "578960446186580977117854925043439539266.34992332820282019728792003956564819967"},
		{"min", "-578960446186580977117854925043439539266.34992332820282019728792003956564819968"},
		{"halfway", "289480223093290488558927462521719769633.17496166410141009864396001978282409984"},
		{"one", "1.0"},
		{"negative_one", "-1.0"},
	}

	pool := memory.NewGoAllocator()
	decimalType := &arrow.Decimal256Type{Precision: 76, Scale: 38}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewDecimal256Builder(pool, decimalType)
			defer builder.Release()

			// Append the value using AppendValueToARROWBuilder
			err := AppendValueToARROWBuilder(&tc.value, builder)
			if err != nil {
				t.Fatalf("AppendValueToARROWBuilder failed: %v", err)
			}

			// Build the array
			arr := builder.NewArray()
			defer arr.Release()

			decimalArr := arr.(*array.Decimal256)
			if decimalArr.Len() != 1 {
				t.Fatalf("expected 1 value, got %d", decimalArr.Len())
			}

			// Get the decimal value and convert to string
			decimalValue := decimalArr.Value(0)
			resultStr := decimalValue.ToString(38) // scale = 38

			// Parse both as big.Rat to compare
			expectedRat := new(big.Rat)
			if _, ok := expectedRat.SetString(tc.value); !ok {
				t.Fatalf("failed to parse expected value: %s", tc.value)
			}

			resultRat := new(big.Rat)
			if _, ok := resultRat.SetString(resultStr); !ok {
				t.Fatalf("failed to parse result value: %s", resultStr)
			}

			if expectedRat.Cmp(resultRat) != 0 {
				t.Errorf("round-trip mismatch:\n  input:    %s\n  output:   %s", tc.value, resultStr)
			}
		})
	}
}
