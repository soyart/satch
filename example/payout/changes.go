package payout

import (
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	CollectionPayouts   = "payouts"
	CollectionAccounts  = "accounts"
	CollectionCustomers = "customers"
)

type Change interface {
	Collection() string
	Filter() bson.M
	SetUpdate(now time.Time) bson.M
}

type Changes struct {
	banned    Set[string]
	suspended Set[string]
	canceled  Set[string]
	settled   Set[string]

	List []Change // Exported for cmd/initdb
}

func (c *Changes) OutputsV2(now time.Time) OutputsV2 {
	colls := make(map[string][]Change)
	for i := range c.List {
		change := c.List[i]
		colls[change.Collection()] = append(colls[change.Collection()], change)
	}

	outputsV2 := make(OutputsV2)

	// Customers will be 1 set-to-ban UpdateMany model
	// Other collections should have multiple UpdateOne
	banned := sort.StringSlice(c.banned.Slice())
	banned.Sort()
	outputsV2[CollectionCustomers] = []mongo.WriteModel{
		mongo.
			NewUpdateManyModel().
			SetFilter(bson.M{
				"id": bson.M{
					"$in": banned,
				},
			}).
			SetUpdate(bson.M{
				"banned":     true,
				"banned_at":  now,
				"updated_at": now,
			}),
	}

	for coll, changes := range colls {
		if coll == CollectionCustomers {
			continue
		}

		writes := make([]mongo.WriteModel, len(changes))
		for i := range changes {
			change := changes[i]
			writes[i] = mongo.
				NewUpdateOneModel().
				SetFilter(change.Filter()).
				SetUpdate(change.SetUpdate(now))
		}

		outputsV2[coll] = writes
	}

	return outputsV2
}

func (c *Changes) OutputsV1(now time.Time) OutputsV1 {
	colls := make(map[string][]Change)
	for i := range c.List {
		change := c.List[i]
		colls[change.Collection()] = append(colls[change.Collection()], change)
	}

	banned := sort.StringSlice(c.banned.Slice())
	banned.Sort()
	writesCusts := mongo.
		NewUpdateManyModel().
		SetFilter(bson.M{
			"id": bson.M{
				"$in": banned,
			},
		}).
		SetUpdate(bson.M{
			"banned":     true,
			"banned_at":  now,
			"updated_at": now,
		})

	changesAccs := colls[CollectionAccounts]
	writesAccs := []mongo.WriteModel{}
	for i := range changesAccs {
		change := changesAccs[i]
		write := mongo.
			NewUpdateOneModel().
			SetFilter(change.Filter()).
			SetUpdate(change.SetUpdate(now))

		writesAccs = append(writesAccs, write)
	}

	changesPayouts := colls[CollectionPayouts]
	writesPayouts := []mongo.WriteModel{}
	for i := range changesPayouts {
		change := changesPayouts[i]
		write := mongo.
			NewUpdateOneModel().
			SetFilter(change.Filter()).
			SetUpdate(change.SetUpdate(now))

		writesPayouts = append(writesPayouts, write)
	}

	return OutputsV1{
		Customers: []mongo.WriteModel{writesCusts},
		Payouts:   writesPayouts,
		Accounts:  writesAccs,
	}
}

func (c *Changes) banCustomer(cust *Customer) {
	cust.Banned = true

	id := cust.ID
	if c.banned.Contains(id) {
		return
	}

	c.banned.Add(id)
	c.List = append(c.List, ChangesCustomerBan(id))
}

func (c *Changes) suspendAccount(acc *Account) {
	acc.Suspended = true

	accNum := acc.Number
	if c.suspended.Contains(accNum) {
		return
	}

	c.suspended.Add(accNum)
	c.List = append(c.List, ChangesAccountSuspend(accNum))
}

func (c *Changes) cancelPayout(p *Payout) {
	id := p.ID

	p.Canceled = true
	p.Settled = false
	c.settled.Delete(id)

	if c.canceled.Contains(id) {
		return
	}

	c.canceled.Add(id)
	c.List = append(c.List, ChangesPayoutCancel(id))
}

func (c *Changes) settlePayout(p *Payout, from, to *Account) {
	if p.Canceled {
		return
	}

	p.Settled = true

	payoutID := p.ID
	if c.settled.Contains(payoutID) {
		return
	}

	settlement := ChangesPayoutSettle(payoutID)

	transferFrom := ChangesAccountTransfer{
		Number: from.Number,
		Amount: 0 - p.Amount,
	}

	transferTo := ChangesAccountTransfer{
		Number: to.Number,
		Amount: p.Amount,
	}

	c.List = append(c.List, transferFrom, transferTo, settlement)
}

func suspendAccountsOfBannedCustomers(inputs Inputs, changes Changes) {
	for i := range inputs.Accounts {
		acc := &inputs.Accounts[i]
		if changes.banned.Contains(acc.OwnerID) {
			changes.suspendAccount(acc)
		}
	}

	for i := range inputs.Customers {
		cust := &inputs.Customers[i]
		if !cust.Banned {
			continue
		}

		for j := range inputs.Accounts {
			acc := &inputs.Accounts[j]
			if acc.OwnerID == cust.ID {
				changes.suspendAccount(acc)
			}
		}
	}
}

func cancelPayoutsWithSuspendedAccounts(inputs Inputs, changes Changes) {
	for i := range inputs.Payouts {
		p := &inputs.Payouts[i]
		from, to := p.From, p.To

		if changes.suspended.Contains(from) {
			changes.cancelPayout(p)
			continue
		}

		if changes.suspended.Contains(to) {
			changes.cancelPayout(p)
		}
	}
}

type (
	ChangesPayoutSettle    string // Payout ID
	ChangesPayoutCancel    string // Payout ID
	ChangesAccountSuspend  string // Account number
	ChangesCustomerBan     string // Customer ID
	ChangesAccountTransfer struct {
		Number string  // Account number
		Amount float64 // Negative for outgoing transfers
	}
)

func (c ChangesPayoutSettle) Collection() string {
	return CollectionPayouts
}

func (c ChangesPayoutSettle) Filter() bson.M {
	return bson.M{
		"number": c,
	}
}

func (c ChangesPayoutSettle) SetUpdate(now time.Time) bson.M {
	return bson.M{
		"settled":    true,
		"settled_at": now,
		"updated_at": now,
	}
}

func (c ChangesPayoutCancel) Collection() string {
	return CollectionPayouts
}

func (c ChangesPayoutCancel) Filter() bson.M {
	return bson.M{
		"number": c,
	}
}

func (c ChangesPayoutCancel) SetUpdate(now time.Time) bson.M {
	return bson.M{
		"canceled":    true,
		"canceled_at": now,
		"updated_at":  now,
	}
}

func (c ChangesAccountSuspend) Collection() string {
	return CollectionAccounts
}

func (c ChangesAccountSuspend) Filter() bson.M {
	return bson.M{
		"number": c,
	}
}

func (c ChangesAccountSuspend) SetUpdate(now time.Time) bson.M {
	return bson.M{
		"suspended":    true,
		"suspended_at": now,
		"updated_at":   now,
	}
}

func (c ChangesCustomerBan) Collection() string {
	return CollectionCustomers
}

func (c ChangesCustomerBan) Filter() bson.M {
	return bson.M{
		"id": c,
	}
}

func (c ChangesCustomerBan) SetUpdate(now time.Time) bson.M {
	return bson.M{
		"banned":     true,
		"banned_at":  now,
		"updated_at": now,
	}
}

func (c ChangesAccountTransfer) Collection() string {
	return CollectionAccounts
}

func (c ChangesAccountTransfer) Filter() bson.M {
	return bson.M{
		"number": c.Number,
	}
}

func (c ChangesAccountTransfer) SetUpdate(now time.Time) bson.M {
	return bson.M{
		"$inc": bson.M{
			"balance": c.Amount,
		},
		"updated_at": now,
	}
}
