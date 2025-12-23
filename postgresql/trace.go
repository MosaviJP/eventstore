package postgresql

import "context"

const traceIDContextKey = "amzn-trace-id"

func traceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v := ctx.Value(traceIDContextKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func traceSuffix(ctx context.Context) string {
	traceID := traceIDFromContext(ctx)
	if traceID == "" {
		return ""
	}
	return " trace_id=" + traceID
}
