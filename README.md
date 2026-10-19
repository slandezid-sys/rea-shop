# shop — rea demo

Order workflow on top of [rea](https://github.com/pithomlabs/rea): reserve stock → charge card, with saga rollback.

- `Inventory` (Virtual Object, key = SKU): `Restock`, `Reserve`, `Release`, `Stock`
- `Payments` (Service): `Charge` (fake provider, declines amounts > 1000)
- `OrderWorkflow` (Workflow, key = order ID): `Run`, `Status`

## Run

```sh
restate-server &                      # npx @restatedev/restate-server
go run . &                            # listens on :9080
restate deployments register http://localhost:9080

curl localhost:8080/Inventory/book/Restock -H 'content-type: application/json' -d '5'
curl localhost:8080/OrderWorkflow/order-1/Run -H 'content-type: application/json' -d '{"sku":"book","qty":2,"price":100}'
curl localhost:8080/OrderWorkflow/order-2/Run -H 'content-type: application/json' -d '{"sku":"book","qty":2,"price":600}'  # declined → rolled back
curl localhost:8080/Inventory/book/Stock    # 3
curl localhost:8080/OrderWorkflow/order-2/Status
```
