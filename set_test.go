package journal

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Laisky/go-utils"
	"github.com/RoaringBitmap/roaring"
)

func TestNewInt64Set(t *testing.T) {
	s := NewInt64Set()
	for i := int64(0); i < 10; i++ {
		s.AddInt64(i)
	}

	for i := int64(5); i < 10; i++ {
		s.CheckAndRemove(i)
	}

	if !s.CheckAndRemove(3) {
		t.Fatal("should contains 3")
	}
	if s.CheckAndRemove(3) {
		t.Fatal("should not contains 3")
	}
	if s.CheckAndRemove(7) {
		t.Fatal("should not contains 7")
	}
}

func TestInt64SetWithTTL(t *testing.T) {
	var err error
	if err = Logger.ChangeLevel("error"); err != nil {
		t.Fatalf("set level: %+v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewInt64SetWithTTL(
		ctx,
		1*time.Second)
	for i := int64(0); i < 10; i++ {
		s.AddInt64(i)
	}

	for i := int64(5); i < 10; i++ {
		if !s.CheckAndRemove(i) {
			t.Fatalf("should contains %d", i)
		}
	}

	for i := int64(5); i < 10; i++ {
		if !s.CheckAndRemove(i) {
			t.Fatalf("should contains %d", i)
		}
	}

	time.Sleep(1100 * time.Millisecond) // all expired
	for i := int64(0); i < 10; i++ {
		if s.CheckAndRemove(i) {
			t.Fatalf("should not contains %d", i)
		}
	}
}

func TestNewUint32Set(t *testing.T) {
	s := NewUint32Set()
	for i := uint32(0); i < 10; i++ {
		s.AddUint32(i)
	}

	for i := uint32(5); i < 10; i++ {
		s.CheckAndRemoveUint32(i)
	}

	if !s.CheckAndRemoveUint32(3) {
		t.Fatal("should contains 3")
	}
	if s.CheckAndRemoveUint32(3) {
		t.Fatal("should not contains 3")
	}
	if s.CheckAndRemoveUint32(7) {
		t.Fatal("should not contains 7")
	}
}

func TestValidateInt64SetWithTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewInt64SetWithTTL(ctx, 1*time.Second)
	wg := &sync.WaitGroup{}
	pool := &sync.Map{}
	padding := struct{}{}

	for nf := 0; nf < 4; nf++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int64
			for i := 0; i < 10000; i++ {
				n = rand.Int63()
				s.AddInt64(n)
				pool.Store(n, padding)
			}
		}()
	}

	wg.Wait()
	pool.Range(func(k, v interface{}) bool {
		if !s.CheckAndRemove(k.(int64)) {
			t.Fatalf("should contains %d", k.(int64))
		}
		return true
	})

	time.Sleep(1100 * time.Millisecond)
	pool.Range(func(k, v interface{}) bool {
		if s.CheckAndRemove(k.(int64)) {
			t.Fatalf("should not contains %d", k.(int64))
		}
		return true
	})

}

func ExampleInt64Set() {
	s := NewInt64Set()
	s.Add(5)

	s.CheckAndRemove(5) // true
	s.CheckAndRemove(3) // false
}

/*
✗ go test -run=All -bench=Int64SetWithTTL -benchtime=5s -benchmem .
BenchmarkInt64SetWithTTL/add-4           5000000              1387 ns/op             187 B/op          4 allocs/op
BenchmarkInt64SetWithTTL/count-4        500000000               19.7 ns/op             0 B/op          0 allocs/op
BenchmarkInt64SetWithTTL/remove-4       50000000               140 ns/op               0 B/op          0 allocs/op
BenchmarkInt64SetWithTTL/parallel-4      2000000              4139 ns/op             348 B/op          8 allocs/op
*/
func BenchmarkInt64SetWithTTL(b *testing.B) {
	var err error
	if err = Logger.ChangeLevel("error"); err != nil {
		b.Fatalf("set level: %+v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewInt64SetWithTTL(
		ctx,
		10*time.Second)
	b.Run("add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.AddInt64(rand.Int63())
		}
	})
	b.Run("count", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.GetLen()
		}
	})
	// b.Run("count v2", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		s.GetLenV2()
	// 	}
	// })
	b.Run("remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.CheckAndRemove(rand.Int63())
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.AddInt64(rand.Int63())
			}
		})
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.AddInt64(rand.Int63())
			}
		})
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.CheckAndRemove(rand.Int63())
			}
		})
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.CheckAndRemove(rand.Int63())
			}
		})
	})
}

/*
✗ go test -run=All -bench=Int64Set -benchtime=5s -benchmem .
BenchmarkInt64Set/add-4         	 1000000	      1064 ns/op	     170 B/op	       4 allocs/op
BenchmarkInt64Set/count-4       	2000000000	         0.37 ns/op	       0 B/op	       0 allocs/op
BenchmarkInt64Set/remove-4      	10000000	       193 ns/op	       0 B/op	       0 allocs/op
BenchmarkInt64Set/parallel-4    	  500000	      4336 ns/op	     343 B/op	       8 allocs/op
*/
func BenchmarkInt64Set(b *testing.B) {
	s := NewInt64Set()
	b.Run("add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.AddInt64(rand.Int63())
		}
	})
	b.Run("count", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.GetLen()
		}
	})
	// b.Run("count v2", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		s.GetLenV2()
	// 	}
	// })
	b.Run("remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.CheckAndRemove(rand.Int63())
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.AddInt64(rand.Int63())
			}
		})
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.AddInt64(rand.Int63())
			}
		})
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.CheckAndRemove(rand.Int63())
			}
		})
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				s.CheckAndRemove(rand.Int63())
			}
		})
	})
}

func BenchmarkMap(b *testing.B) {
	m := map[string]struct{}{}
	sm := sync.Map{}
	// s := mapset.NewSet()
	rm := roaring.New()
	var k string
	b.Run("map add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			m[utils.RandomStringWithLength(20)] = struct{}{}
		}
	})
	b.Run("sync map add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sm.Store(utils.RandomStringWithLength(20), struct{}{})
		}
	})
	b.Run("bitmap add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rm.AddInt(rand.Int())
		}
	})
	// b.Run("set add", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		s.Add(utils.RandomStringWithLength(20))
	// 	}
	// })
	b.Run("map get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k = utils.RandomStringWithLength(20)
			_ = m[k]
			delete(m, k)
		}
	})
	b.Run("sync map get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k = utils.RandomStringWithLength(20)
			sm.Load(k)
			sm.Delete(k)
		}
	})
	b.Run("bitmap get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rm.ContainsInt(rand.Int())
			rm.Remove(rand.Uint32())
		}
	})
	// b.Run("set get", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		k = utils.RandomStringWithLength(20)
	// 		s.Contains(k)
	// 		s.Remove(k)
	// 	}
	// })
}

func BenchmarkSet(b *testing.B) {
	s1 := &sync.Map{}
	s2 := &sync.Map{}
	load := struct{}{}
	var k int
	b.Run("simple", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s1.Store(rand.Int(), load)
		}
	})
	b.Run("simple remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k = rand.Int()
			s1.Load(k)
			s1.Delete(k)
		}
	})
	b.Run("bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s2.Store(rand.Int(), true)
		}
	})
	b.Run("bool remove", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			k = rand.Int()
			s2.Load(k)
			s2.Store(k, false)
		}
	})
}
