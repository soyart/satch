package payout

import (
	"time"
)

type Payout struct {
	ID         string  `bson:"id" json:"id"`
	From       string  `bson:"from" json:"from"`
	To         string  `bson:"to" json:"to"`
	Remarks    string  `bson:"remarks,omitempty" json:"remarks,omitempty"`
	T          int64   `bson:"t" json:"t"`
	CreatedAt  int64   `bson:"created_at" json:"createdAt,omitempty"`
	UpdatedAt  int64   `bson:"updated_at,omitempty" json:"updatedAt,omitempty"`
	SettledAt  int64   `bson:"settled_at,omitempty" json:"settledAt,omitempty"`
	CanceledAt int64   `bson:"canceled_at,omitempty" json:"canceledAt,omitempty"`
	Amount     float64 `bson:"amount" json:"amount"`
	Settled    bool    `bson:"settled,omitempty" json:"settled,omitempty"`
	Canceled   bool    `bson:"canceled,omitempty" json:"canceled,omitempty"`
}

type Account struct {
	Number      string  `bson:"number" json:"number,omitempty"`
	OwnerID     string  `bson:"owner_id" json:"ownerID,omitempty"`
	CreatedAt   int64   `bson:"created_at" json:"createdAt"`
	UpdatedAt   int64   `bson:"updated_at,omitempty" json:"updatedAt,omitempty"`
	SuspendedAt int64   `bson:"suspended_at,omitempty" json:"suspendedAt,omitempty"`
	Balance     float64 `bson:"balance" json:"balance"`
	Suspended   bool    `bson:"suspended,omitempty" json:"suspended,omitempty"`
}

type Customer struct {
	ID        string `bson:"id" json:"id"`
	Name      string `bson:"name" json:"name"`
	CreatedAt int64  `bson:"created_at" json:"createdAt"`
	UpdatedAt int64  `bson:"updated_at,omitempty" json:"updatedAt,omitempty"`
	BannedAt  int64  `bson:"banned_at" json:"bannedAt,omitempty"`
	Banned    bool   `bson:"banned,omitempty" json:"banned"`
	Criminal  bool   `bson:"criminal,omitempty" json:"criminal"`
}

// Pays out on T+1, while also banning suspicious customers,
// suspending suspicious accounts, and canceling bad payouts.
//
// ProcessPayout also modifies argument values
func ProcessPayout(
	inputs Inputs,
	now time.Time,
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

	tPlusTwo := inputs.CutOffT.Add(48 * time.Hour)

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

		if time.Unix(p.T, 0).After(tPlusTwo) {
			continue
		}

		changes.settlePayout(p, from, to)
	}

	suspendAccountsOfBannedCustomers(inputs, changes)
	cancelPayoutsWithSuspendedAccounts(inputs, changes)

	return changes, nil
}
