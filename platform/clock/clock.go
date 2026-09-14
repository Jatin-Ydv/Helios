// Package clock abstracts time so domain logic and saga timeouts are testable
// without sleeping. Production uses System; tests inject a Fake.
package clock

import "time"

type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Since(t time.Time) time.Duration
}

type System struct{}

func (System) Now() time.Time                         { return time.Now().UTC() }
func (System) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (System) Since(t time.Time) time.Duration        { return time.Since(t) }

type Fake struct {
	T time.Time
}

func (f *Fake) Now() time.Time                  { return f.T }
func (f *Fake) Since(t time.Time) time.Duration { return f.T.Sub(t) }
func (f *Fake) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- f.T.Add(d)
	return ch
}
func (f *Fake) Advance(d time.Duration) { f.T = f.T.Add(d) }
