package postgresql

import "context"

const traceIDContextKey = "trace-id"
const rootTraceIDContextKey = "root-trace-id"

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

func rootTraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v := ctx.Value(rootTraceIDContextKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func traceSuffix(ctx context.Context) string {
	traceID := traceIDFromContext(ctx)
	rootTraceID := rootTraceIDFromContext(ctx)
	if traceID == "" && rootTraceID == "" {
		return ""
	}
	if traceID == "" {
		return " root_trace_id=" + rootTraceID
	}
	if rootTraceID == "" {
		return " trace_id=" + traceID
	}
	return " trace_id=" + traceID + " root_trace_id=" + rootTraceID
}
