package ledger

// logRootCopy returns a defensive copy of the current epoch's log root. Tests
// need it to assert on the sealed index after a seal; the projection
// [Reader.Root] hides all of that.
func (r *Reader) logRootCopy() epochLogRoot {
	root := r.logRoot
	root.Sealed = append([]sealedRef(nil), r.logRoot.Sealed...)
	return root
}

// logRootCopy returns a defensive copy of the current epoch log root — its
// sealed index and open region — which the projection [Writer.Root] hides.
func (w *Writer) logRootCopy() epochLogRoot {
	w.mu.Lock()
	defer w.mu.Unlock()

	root := w.logRoot
	root.Sealed = append([]sealedRef(nil), w.logRoot.Sealed...)

	return root
}
