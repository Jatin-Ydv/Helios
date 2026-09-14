package money_test

import (
	"testing"

	"github.com/helios-commerce/helios/platform/money"
)

func TestAddSameCurrency(t *testing.T) {
	a := money.New(1999, "USD")
	b := money.New(1, "USD")
	got, err := a.Add(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Amount != 2000 {
		t.Fatalf("got %d", got.Amount)
	}
}

func TestAddMismatch(t *testing.T) {
	_, err := money.New(1, "USD").Add(money.New(1, "EUR"))
	if err != money.ErrCurrencyMismatch {
		t.Fatalf("expected mismatch, got %v", err)
	}
}

func TestMulQty(t *testing.T) {
	got, err := money.New(250, "USD").MulQty(4)
	if err != nil || got.Amount != 1000 {
		t.Fatalf("got %+v err %v", got, err)
	}
}
