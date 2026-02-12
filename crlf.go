package main

import "io"

// CRLFWriter is an adapter that replaces the final \n with \r\n
type CRLFWriter struct {
	Out io.Writer
}

func (w CRLFWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// slog guarantees that log entries end with \n.
	// If we see it, we replace it.
	if p[len(p)-1] == '\n' {
		// 1. Write everything UP TO the newline
		if _, err := w.Out.Write(p[:len(p)-1]); err != nil {
			return 0, err
		}
		// 2. Write the Carriage Return + Line Feed
		if _, err := w.Out.Write([]byte("\r\n")); err != nil {
			return len(p) - 1, err
		}
		// Return the length of the original slice so the caller (slog)
		// thinks everything went according to plan.
		return len(p), nil
	}

	// Fallback for partial writes or non-terminated lines
	return w.Out.Write(p)
}
