package server

import (
	"strconv"

	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// flexibleQueryRequest wraps bigqueryv2.QueryRequest to handle flexible unmarshalling
// of query parameter values. The Node.js BigQuery client sends numeric values as JSON
// numbers, but the generated QueryParameterValue struct expects all values as strings.
type flexibleQueryRequest struct {
	bigqueryv2.QueryRequest
}

// UnmarshalJSON implements custom unmarshalling that converts numeric parameter values
// to strings to match the expected QueryParameterValue.Value type.
func (f *flexibleQueryRequest) UnmarshalJSON(data []byte) error {
	// First unmarshal into a generic map to inspect the structure
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Process queryParameters if present
	var nullValuePaths [][]int
	if params, ok := raw["queryParameters"].([]interface{}); ok {
		nullValuePaths = processQueryParameters(params)
	}

	// Marshal the modified structure back to JSON
	modified, err := json.Marshal(raw)
	if err != nil {
		return err
	}

	// Unmarshal into the embedded QueryRequest using a type alias to avoid recursion
	type queryRequestAlias bigqueryv2.QueryRequest
	if err := json.Unmarshal(modified, (*queryRequestAlias)(&f.QueryRequest)); err != nil {
		return err
	}

	// Apply NullFields to parameter values that had null scalar values
	for _, path := range nullValuePaths {
		if len(path) > 0 && path[0] < len(f.QueryRequest.QueryParameters) {
			applyNullField(f.QueryRequest.QueryParameters[path[0]].ParameterValue, path[1:])
		}
	}

	return nil
}

// flexibleJob wraps bigqueryv2.Job to handle flexible unmarshalling
// of query parameter values in job configurations.
type flexibleJob struct {
	bigqueryv2.Job
}

// UnmarshalJSON implements custom unmarshalling that converts numeric parameter values
// to strings to match the expected QueryParameterValue.Value type.
func (f *flexibleJob) UnmarshalJSON(data []byte) error {
	// First unmarshal into a generic map to inspect the structure
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Process configuration.query.queryParameters if present
	var nullValuePaths [][]int
	if config, ok := raw["configuration"].(map[string]interface{}); ok {
		if query, ok := config["query"].(map[string]interface{}); ok {
			if params, ok := query["queryParameters"].([]interface{}); ok {
				nullValuePaths = processQueryParameters(params)
			}
		}
	}

	// Marshal the modified structure back to JSON
	modified, err := json.Marshal(raw)
	if err != nil {
		return err
	}

	// Unmarshal into the embedded Job using a type alias to avoid recursion
	type jobAlias bigqueryv2.Job
	if err := json.Unmarshal(modified, (*jobAlias)(&f.Job)); err != nil {
		return err
	}

	// Apply NullFields to parameter values that had null scalar values
	if f.Job.Configuration != nil && f.Job.Configuration.Query != nil {
		for _, path := range nullValuePaths {
			if len(path) > 0 && path[0] < len(f.Job.Configuration.Query.QueryParameters) {
				applyNullField(f.Job.Configuration.Query.QueryParameters[path[0]].ParameterValue, path[1:])
			}
		}
	}

	return nil
}

// processQueryParameters handles both null tracking and normalization of query parameters.
// It processes all parameters in the list, collecting paths to null values and normalizing
// numeric/boolean values to strings. Returns the paths to parameters with null values.
func processQueryParameters(params []interface{}) [][]int {
	var nullValuePaths [][]int

	for i := range params {
		if paramMap, ok := params[i].(map[string]interface{}); ok {
			if paramValue, ok := paramMap["parameterValue"].(map[string]interface{}); ok {
				collectNullPaths(paramValue, []int{i}, &nullValuePaths)
				normalizeParameterValue(paramValue)
			}
		}
	}

	return nullValuePaths
}

// collectNullPaths walks through a parameter value structure and records paths
// to any scalar values that are null. This information is used later to set
// NullFields on the unmarshaled struct.
func collectNullPaths(paramValue map[string]interface{}, currentPath []int, nullPaths *[][]int) {
	// Check if the scalar value field is null
	if value, ok := paramValue["value"]; ok && value == nil {
		// Record this path as having a null value
		pathCopy := make([]int, len(currentPath))
		copy(pathCopy, currentPath)
		*nullPaths = append(*nullPaths, pathCopy)
	}

	// Recursively check array values
	if arrayValues, ok := paramValue["arrayValues"].([]interface{}); ok {
		for i, arrVal := range arrayValues {
			if arrValMap, ok := arrVal.(map[string]interface{}); ok {
				collectNullPaths(arrValMap, append(currentPath, i), nullPaths)
			}
		}
	}

	// Recursively check struct values (note: struct values are keyed by field name, not index)
	// For simplicity, we'll handle this separately if needed
}

// applyNullField sets the NullFields on a QueryParameterValue to indicate
// that the Value field should be serialized as null.
func applyNullField(pv *bigqueryv2.QueryParameterValue, path []int) {
	if pv == nil {
		return
	}

	if len(path) == 0 {
		// We've reached the target - mark Value as null
		pv.NullFields = append(pv.NullFields, "Value")
		return
	}

	// Navigate deeper into the structure
	nextIndex := path[0]
	remainingPath := path[1:]

	// Check if we're navigating through array values
	if nextIndex < len(pv.ArrayValues) {
		applyNullField(pv.ArrayValues[nextIndex], remainingPath)
	}
}

// normalizeParameterValue recursively converts numeric values to strings in parameter values
func normalizeParameterValue(paramValue map[string]interface{}) {
	// Handle the scalar value field
	if value, ok := paramValue["value"]; ok {
		switch v := value.(type) {
		case float64:
			// JSON numbers are unmarshalled as float64
			// Convert to string, using integer format if it's a whole number
			if v == float64(int64(v)) {
				paramValue["value"] = strconv.FormatInt(int64(v), 10)
			} else {
				paramValue["value"] = strconv.FormatFloat(v, 'f', -1, 64)
			}
		case bool:
			// Convert booleans to string
			paramValue["value"] = strconv.FormatBool(v)
		case string:
			// Already a string, no conversion needed
		case nil:
			// Null value, leave as is
		}
	}

	// Handle array values recursively
	if arrayValues, ok := paramValue["arrayValues"].([]interface{}); ok {
		for _, arrVal := range arrayValues {
			if arrValMap, ok := arrVal.(map[string]interface{}); ok {
				normalizeParameterValue(arrValMap)
			}
		}
	}

	// Handle struct values recursively
	if structValues, ok := paramValue["structValues"].(map[string]interface{}); ok {
		for _, structVal := range structValues {
			if structValMap, ok := structVal.(map[string]interface{}); ok {
				normalizeParameterValue(structValMap)
			}
		}
	}
}
