package optional

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
)

type optionalTagT byte

const (
	optionalNoneTag optionalTagT = iota
	optionalSomeTag
)

type Optional[T any] struct {
	tag   optionalTagT
	value T
}

func Some[T any](value T) Optional[T] {
	return Optional[T]{
		tag:   optionalSomeTag,
		value: value,
	}
}

func None[T any]() Optional[T] {
	return Optional[T]{
		tag:   optionalNoneTag,
		value: *new(T),
	}
}

func (opt *Optional[T]) Emplace(value T) {
	opt.tag = optionalSomeTag
	opt.value = value
}

func (opt *Optional[T]) Clear() {
	opt.tag = optionalNoneTag
	opt.value = *new(T) // for deep equality
}

func (opt *Optional[T]) Expect(msg string) T {
	assert.Assert(opt.tag != optionalNoneTag, msg)
	return opt.value
}

func (opt *Optional[T]) Unwrap() T {
	assert.Assert(opt.tag != optionalNoneTag)
	return opt.value
}

func (opt *Optional[T]) IsNone() bool {
	return opt.tag == optionalNoneTag
}

func (opt *Optional[T]) IsSome() bool {
	return opt.tag != optionalNoneTag
}
