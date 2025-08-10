package utils

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

type WithUnlock[T any] struct {
	Resource T
	UnlockFn func() error
}

func (w *WithUnlock[T]) Unlock() error {
	if w.UnlockFn != nil {
		return w.UnlockFn()
	}

	return nil
}
