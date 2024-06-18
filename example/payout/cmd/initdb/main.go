package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/soyart/satch/datasource/satchmongo"
	"github.com/soyart/satch/example/payout"
)

func main() {
	ctx := context.Background()

	mg, err := satchmongo.NewMongoDB(ctx, satchmongo.MongoDBConfig{
		Hosts:    "localhost:47017",
		Admin:    "admin",
		Username: "test_user",
		Password: "test_password",
	})
	if err != nil {
		panic(err)
	}

	db := "example-payout"

	collCustomers := mg.Collection(db, "customers")
	// collPayouts := mg.Collection(db, "payouts")
	// collAccounts := mg.Collection(db, "accounts")
	// _, _, _ = collPayouts, collAccounts, collCustomers

	custs := mockCustomers(0, 10, trueFn, trueFn)
	log.Println("writing insertMany customers")

	result, err := collCustomers.InsertMany(ctx, sliceInterface(custs))
	if err != nil {
		log.Println("error inserting many customers")
		panic(err)
	}

	log.Println("resultFind", "type", reflect.TypeOf(result).String(), "value", result)
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
	for i := start; i < end; i++ {
		custs[i] = payout.Customer{
			ID:       fmt.Sprintf("cust_%d", i),
			Name:     fmt.Sprintf("custname_%d", i),
			Banned:   banFn(i),
			Criminal: crimeFn(i),
		}
	}

	return custs
}

func mockAccounts(customers []payout.Customer, accFunc func(*payout.Customer) []payout.Account) []payout.Account {
	accounts := []payout.Account{}
	for i := range customers {
		cust := &customers[i]
		accounts = append(accounts, accFunc(cust)...)
	}

	return accounts
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
