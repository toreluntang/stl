package httpclient

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsNonRetryable(t *testing.T) {
	inner := errors.New("boom")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain error", err: inner, want: false},
		{name: "direct", err: WrapNonRetryable(inner), want: true},
		{name: "single wrap", err: fmt.Errorf("outer: %w", WrapNonRetryable(inner)), want: true},
		{name: "double wrap", err: fmt.Errorf("a: %w", fmt.Errorf("b: %w", WrapNonRetryable(inner))), want: true},
		{
			name: "joined with non-retryable",
			err:  errors.Join(errors.New("transient"), WrapNonRetryable(inner)),
			want: true,
		},
		{
			name: "joined without non-retryable",
			err:  errors.Join(errors.New("a"), errors.New("b")),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target *NonRetryableError
			if got := isNonRetryable(tt.err, &target); got != tt.want {
				t.Errorf("isNonRetryable = %v, want %v", got, tt.want)
			}
			if tt.want && target == nil {
				t.Error("expected target to be populated")
			}
			if !tt.want && target != nil {
				t.Error("expected target to stay nil")
			}
		})
	}
}
