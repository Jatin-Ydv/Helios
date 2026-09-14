// Package money is a strict integer-cents representation.
//
// Floating point is forbidden for currency. All prices, authorizations and
// captures are int64 minor units (cents for USD/EUR). Cross-currency conversion
// is explicitly out of scope of a single Money value — that is an FX service
// concern, not an implicit multiply.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrNegativeAmount   = errors.New("money: amount must be >= 0 for this operation")
	ErrOverflow         = errors.New("money: arithmetic overflow")
)

type Money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

func New(amount int64, currency string) Money {
	return Money{Amount: amount, Currency: currency}
}

func Zero(currency string) Money { return Money{Amount: 0, Currency: currency} }

func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, ErrCurrencyMismatch
	}
	sum := m.Amount + other.Amount
	if (other.Amount > 0 && sum < m.Amount) || (other.Amount < 0 && sum > m.Amount) {
		return Money{}, ErrOverflow
	}
	return Money{Amount: sum, Currency: m.Currency}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, ErrCurrencyMismatch
	}
	return Money{Amount: m.Amount - other.Amount, Currency: m.Currency}, nil
}

func (m Money) MulQty(qty int64) (Money, error) {
	if qty < 0 {
		return Money{}, ErrNegativeAmount
	}
	if m.Amount != 0 && qty > (1<<63-1)/abs(m.Amount) {
		return Money{}, ErrOverflow
	}
	return Money{Amount: m.Amount * qty, Currency: m.Currency}, nil
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func (m Money) IsZero() bool { return m.Amount == 0 }

func (m Money) String() string {
	return fmt.Sprintf("%s %d.%02d", m.Currency, m.Amount/100, abs(m.Amount%100))
}

func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Amount   int64  `json:"amount"`
		Currency string `json:"currency"`
		Decimal  string `json:"decimal"`
	}{m.Amount, m.Currency, fmt.Sprintf("%d.%02d", m.Amount/100, abs(m.Amount%100))})
}
