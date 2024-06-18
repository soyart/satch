package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/soyart/satch/example/payout"
)

func main() {
	pront := func(j []byte) {
		fmt.Println("--custs--")
		fmt.Println(string(j))
		fmt.Println("---------")
	}

	custs := mockCustomers(0, 17*2, falseFn, falseFn)
	j, _ := json.Marshal(custs)
	pront(j)

	accounts := mockAccounts(custs, mockAccountsForCustomer)
	j, _ = json.Marshal(accounts)
	pront(j)

	fmt.Println("now unixTime", time.Now().Unix())

	// payouts are mocked manually
}

func trueFn(int) bool  { return true }
func falseFn(int) bool { return false }

func sliceInterface[T any](slice []T) []interface{} {
	result := make([]interface{}, len(slice))
	for i := range slice {
		result[i] = slice[i]
	}

	return result
}

func mockCustomers(start, end int, banFn, crimeFn func(int) bool) []payout.Customer {
	n := end - start
	custs := make([]payout.Customer, n)
	bankFoundedStr := "2020-03-27"
	bankFounded, err := time.Parse("2006-01-02", bankFoundedStr)
	if err != nil {
		panic(err)
	}

	for i := start; i < end; i++ {
		createdAt := bankFounded.Add(time.Duration(i*24) * time.Hour)
		custs[i] = payout.Customer{
			ID:        fmt.Sprintf("cust_%d", i),
			Name:      fmt.Sprintf("custname_%d", i),
			Banned:    banFn(i),
			Criminal:  crimeFn(i),
			CreatedAt: createdAt.Unix(),
		}
	}

	return custs
}

func mockAccounts(customers []payout.Customer, accsFunc func(cust *payout.Customer, maxAccountsPerCust int, mod int) []payout.Account) []payout.Account {
	accounts := []payout.Account{}
	number := 0
	for i := range customers {
		cust := &customers[i]
		accs := accsFunc(cust, number, 3)

		for j := range accs {
			accs[j].Number = fmt.Sprintf("acc_%d", number)
			accounts = append(accounts, accs[j])
			number++
		}
	}

	return accounts
}

func mockAccountsForCustomer(cust *payout.Customer, n int, mod int) []payout.Account {
	x := n % mod

	custNum := extractTrailingInt(cust.ID)
	countAccs := x + 1
	accs := make([]payout.Account, countAccs)

	for i := 0; i < countAccs; i++ {
		suspended := i%mod == 0

		var suspendedAt int64
		if suspended {
			suspendedAt = unixTime(cust.CreatedAt).Add(time.Duration(custNum%mod) * time.Hour).Unix()
		}

		accs[i] = payout.Account{
			OwnerID:     cust.ID,
			CreatedAt:   unixTime(cust.CreatedAt).Add(time.Duration(x) * time.Minute).Unix(),
			UpdatedAt:   0,
			SuspendedAt: suspendedAt,
			Balance:     float64(((x+custNum)*19)%17) * 1000,
			Suspended:   suspended,
		}
	}

	return accs
}

func extractTrailingInt(s string) int {
	parts := strings.Split(s, "_")
	if len(parts) < 2 {
		panic("bad string")
	}

	i, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		panic(err)
	}

	return int(i)
}

func unixTime(i int64) time.Time {
	return time.Unix(i, 0)
}
