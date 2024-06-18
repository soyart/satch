package payout

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(item T) {
	s[item] = struct{}{}
}

func (s Set[T]) Delete(item T) {
	delete(s, item)
}

func (s Set[T]) Contains(item T) bool {
	_, ok := s[item]
	return ok
}

func (s Set[T]) Slice() []T {
	slice := make([]T, len(s))
	c := 0
	for k := range s {
		slice[c] = k
		c++
	}

	return slice
}
