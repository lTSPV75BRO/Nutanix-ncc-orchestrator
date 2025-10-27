package errors

import (
	"fmt"
	"net/http"
)

// ErrorType represents the type of error
type ErrorType string

const (
	ErrorTypeConfig     ErrorType = "config"
	ErrorTypeNetwork    ErrorType = "network"
	ErrorTypeAuth       ErrorType = "auth"
	ErrorTypeParse      ErrorType = "parse"
	ErrorTypeFile       ErrorType = "file"
	ErrorTypeValidation ErrorType = "validation"
	ErrorTypeTimeout    ErrorType = "timeout"
	ErrorTypeUnknown    ErrorType = "unknown"
)

// NCCError represents a custom error with context
type NCCError struct {
	Type    ErrorType
	Message string
	Err     error
	Context map[string]interface{}
}

// Error implements the error interface
func (e *NCCError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Type, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Type, e.Message)
}

// Unwrap returns the underlying error
func (e *NCCError) Unwrap() error {
	return e.Err
}

// WithContext adds context to the error
func (e *NCCError) WithContext(key string, value interface{}) *NCCError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// New creates a new NCCError
func New(errType ErrorType, message string) *NCCError {
	return &NCCError{
		Type:    errType,
		Message: message,
		Context: make(map[string]interface{}),
	}
}

// Wrap wraps an existing error
func Wrap(err error, errType ErrorType, message string) *NCCError {
	return &NCCError{
		Type:    errType,
		Message: message,
		Err:     err,
		Context: make(map[string]interface{}),
	}
}

// IsRetryable checks if an error is retryable
func (e *NCCError) IsRetryable() bool {
	switch e.Type {
	case ErrorTypeNetwork, ErrorTypeTimeout:
		return true
	default:
		return false
	}
}

// HTTPError represents an HTTP-related error
type HTTPError struct {
	*NCCError
	StatusCode int
	URL        string
}

// NewHTTPError creates a new HTTP error
func NewHTTPError(statusCode int, url, message string) *HTTPError {
	return &HTTPError{
		NCCError:   New(ErrorTypeNetwork, message),
		StatusCode: statusCode,
		URL:        url,
	}
}

// IsRetryableStatus checks if an HTTP status code is retryable
func IsRetryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

// ConfigError represents a configuration error
func ConfigError(message string) *NCCError {
	return New(ErrorTypeConfig, message)
}

// ConfigErrorf creates a formatted configuration error
func ConfigErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeConfig, fmt.Sprintf(format, args...))
}

// NetworkError represents a network error
func NetworkError(message string) *NCCError {
	return New(ErrorTypeNetwork, message)
}

// NetworkErrorf creates a formatted network error
func NetworkErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeNetwork, fmt.Sprintf(format, args...))
}

// AuthError represents an authentication error
func AuthError(message string) *NCCError {
	return New(ErrorTypeAuth, message)
}

// AuthErrorf creates a formatted authentication error
func AuthErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeAuth, fmt.Sprintf(format, args...))
}

// ParseError represents a parsing error
func ParseError(message string) *NCCError {
	return New(ErrorTypeParse, message)
}

// ParseErrorf creates a formatted parsing error
func ParseErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeParse, fmt.Sprintf(format, args...))
}

// FileError represents a file system error
func FileError(message string) *NCCError {
	return New(ErrorTypeFile, message)
}

// FileErrorf creates a formatted file error
func FileErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeFile, fmt.Sprintf(format, args...))
}

// ValidationError represents a validation error
func ValidationError(message string) *NCCError {
	return New(ErrorTypeValidation, message)
}

// ValidationErrorf creates a formatted validation error
func ValidationErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeValidation, fmt.Sprintf(format, args...))
}

// TimeoutError represents a timeout error
func TimeoutError(message string) *NCCError {
	return New(ErrorTypeTimeout, message)
}

// TimeoutErrorf creates a formatted timeout error
func TimeoutErrorf(format string, args ...interface{}) *NCCError {
	return New(ErrorTypeTimeout, fmt.Sprintf(format, args...))
}
