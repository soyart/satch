package payout

import (
	"time"
)

type Payout struct {
	T          time.Time `bson:"t"` // Settlement date
	CreatedAt  time.Time `bson:"created_at"`
	UpdatedAt  time.Time `bson:"updated_at,omitempty"`
	SettledAt  time.Time `bson:"settled_at,omitempty"`
	CanceledAt time.Time `bson:"canceled_at,omitempty"`
	ID         string    `bson:"id"`
	From       string    `bson:"from"`
	To         string    `bson:"to"`
	Remarks    string    `bson:"remarks,omitempty"`
	Amount     float64   `bson:"amount"`
	Settled    bool      `bson:"settled,omitempty"`
	Canceled   bool      `bson:"canceled,omitempty"`
}

type Account struct {
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at,omitempty"`
	SuspendedAt time.Time `bson:"suspended_at,omitempty"`
	Number      string    `bson:"number"`
	OwnerID     string    `bson:"owner_id"`
	Balance     float64   `bson:"balance"`
	Suspended   bool      `bson:"suspended,omitempty"`
}

type Customer struct {
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at,omitempty"`
	BannedAt  time.Time `bson:"banned_at"`
	ID        string    `bson:"id"`
	Name      string    `bson:"name"`
	Banned    bool      `bson:"banned,omitempty"`
	Criminal  bool      `bson:"criminal,omitempty"`
}

// Pays out on T+1, while also banning suspicious customers,
// suspending suspicious accounts, and canceling bad payouts.
//
// ProcessPayout also modifies argument values
func ProcessPayout(
	inputs Inputs,
) (
	Changes,
	error,
) {
	payouts := make([]Payout, 0, len(inputs.Payouts))
	customers := make(map[string]*Customer)
	accounts := make(map[string]*Account)

	// Init data
	// We'll copy inputs to payouts, and then leave inputs unchanged
	{
		copy(payouts, inputs.Payouts)

		for i := range inputs.Customers {
			cust := inputs.Customers[i]
			customers[cust.ID] = &cust
		}

		for i := range inputs.Accounts {
			acc := inputs.Accounts[i]
			accounts[acc.Number] = &acc
		}
	}

	changes := Changes{
		List:      make([]Change, 0),
		banned:    make(Set[string]),
		suspended: make(Set[string]),
		canceled:  make(Set[string]),
	}

	tPlusTwo := inputs.T.Add(48 * time.Hour)

	for i := range payouts {
		p := &payouts[i]

		from, ok := accounts[p.From]
		if !ok {
			changes.cancelPayout(p)
		}

		to, ok := accounts[p.To]
		if !ok {
			changes.cancelPayout(p)
		}

		if from.Suspended {
			changes.cancelPayout(p)
		}

		if to.Suspended {
			changes.cancelPayout(p)
		}

		// Handle suspicious entities

		fromCust, ok := customers[from.OwnerID]
		if !ok {
			changes.cancelPayout(p)
		}

		toCust, ok := customers[to.OwnerID]
		if !ok {
			changes.cancelPayout(p)
		}

		if fromCust.Banned {
			changes.cancelPayout(p)
			changes.suspendAccount(from)
		}

		if toCust.Banned {
			changes.cancelPayout(p)
			changes.suspendAccount(to)
		}

		if fromCust.Criminal {
			changes.cancelPayout(p)
			changes.banCustomer(toCust)
			changes.suspendAccount(to)
		}

		if toCust.Criminal {
			changes.cancelPayout(p)
			changes.banCustomer(fromCust)
			changes.suspendAccount(from)
		}

		switch {
		case
			changes.canceled.Contains(p.ID),
			changes.suspended.Contains(from.Number),
			changes.suspended.Contains(to.Number),
			changes.banned.Contains(fromCust.ID),
			changes.banned.Contains(toCust.ID):

			continue
		}

		// Transfer and settle
		if from.Balance < p.Amount {
			changes.cancelPayout(p)
			continue
		}

		if p.T.After(tPlusTwo) {
			continue
		}

		changes.settlePayout(p, from, to)
	}

	suspendAccountsOfBannedCustomers(inputs, changes)
	cancelPayoutsWithSuspendedAccounts(inputs, changes)

	return changes, nil
}
