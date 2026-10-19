// Command shop is a small demo of the rea framework: an order workflow that
// reserves stock, charges a card and rolls everything back via a saga when a
// step fails.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/pithomlabs/rea"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
)

// -----------------------------------------------------------------------------
// Data plane: Inventory (Virtual Object, keyed by SKU)
// -----------------------------------------------------------------------------

type Inventory struct{}

type ReserveRequest struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

func (Inventory) Restock(ctx restate.ObjectContext, qty int) (int, error) {
	stock := rea.NewState[int](ctx, "stock")
	cur, err := stock.Get()
	if err != nil {
		return 0, err
	}
	cur += qty
	return cur, stock.Set(cur)
}

// Reserve is idempotent per order: a second reservation for the same order is a no-op.
func (Inventory) Reserve(ctx restate.ObjectContext, req ReserveRequest) (int, error) {
	stock := rea.NewState[int](ctx, "stock")
	reserved := rea.NewState[map[string]int](ctx, "reserved")

	cur, err := stock.Get()
	if err != nil {
		return 0, err
	}
	res, err := reserved.Get()
	if err != nil {
		return 0, err
	}
	if res == nil {
		res = map[string]int{}
	}
	if _, ok := res[req.OrderID]; ok {
		return cur, nil
	}
	if req.Qty <= 0 || cur < req.Qty {
		return cur, rea.NewTerminalError(fmt.Errorf("out of stock: sku=%s have=%d want=%d", restate.Key(ctx), cur, req.Qty))
	}

	res[req.OrderID] = req.Qty
	if err := reserved.Set(res); err != nil {
		return 0, err
	}
	return cur - req.Qty, stock.Set(cur - req.Qty)
}

// Release returns a reservation to stock. Idempotent: releasing twice is a no-op.
func (Inventory) Release(ctx restate.ObjectContext, orderID string) (int, error) {
	stock := rea.NewState[int](ctx, "stock")
	reserved := rea.NewState[map[string]int](ctx, "reserved")

	cur, err := stock.Get()
	if err != nil {
		return 0, err
	}
	res, err := reserved.Get()
	if err != nil {
		return 0, err
	}
	qty, ok := res[orderID]
	if !ok {
		return cur, nil
	}
	delete(res, orderID)
	if err := reserved.Set(res); err != nil {
		return 0, err
	}
	return cur + qty, stock.Set(cur + qty)
}

func (Inventory) Stock(ctx restate.ObjectSharedContext) (int, error) {
	return rea.NewState[int](ctx, "stock").Get()
}

// -----------------------------------------------------------------------------
// Data plane: Payments (stateless Service, fake card provider)
// -----------------------------------------------------------------------------

type Payments struct{}

type ChargeRequest struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

type Receipt struct {
	PaymentID string `json:"payment_id"`
	Amount    int    `json:"amount"`
}

// cardLimit makes it easy to trigger a declined payment and watch the saga roll back.
const cardLimit = 1000

func (Payments) Charge(ctx restate.Context, req ChargeRequest) (Receipt, error) {
	return rea.RunDo(ctx, func(rc restate.RunContext) (Receipt, error) {
		return providerCharge(rc, req)
	}, restate.WithName("provider.charge"))
}

// providerCharge and providerRefund stand in for a real card provider API.
func providerCharge(rc restate.RunContext, req ChargeRequest) (Receipt, error) {
	if req.Amount > cardLimit {
		return Receipt{}, rea.NewTerminalError(fmt.Errorf("card declined: amount %d exceeds limit %d", req.Amount, cardLimit))
	}
	rc.Log().Info("provider: charge", "order", req.OrderID, "amount", req.Amount)
	return Receipt{PaymentID: "pay-" + req.OrderID, Amount: req.Amount}, nil
}

// providerRefund is idempotent on the provider side (keyed by order ID).
func providerRefund(rc restate.RunContext, orderID string) error {
	rc.Log().Info("provider: refund", "order", orderID)
	return nil
}

// -----------------------------------------------------------------------------
// Control plane: OrderWorkflow (Workflow, keyed by order ID)
// -----------------------------------------------------------------------------

type OrderWorkflow struct{}

type Order struct {
	SKU   string `json:"sku"`
	Qty   int    `json:"qty"`
	Price int    `json:"price"`
}

type OrderResult struct {
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	PaymentID string `json:"payment_id,omitempty"`
}

var (
	reserveStock = rea.ObjectClient[ReserveRequest, int]{ServiceName: "Inventory", HandlerName: "Reserve"}
	releaseStock = rea.ObjectClient[string, int]{ServiceName: "Inventory", HandlerName: "Release"}
	chargeCard   = rea.ServiceClient[ChargeRequest, Receipt]{ServiceName: "Payments", HandlerName: "Charge"}
)

func (OrderWorkflow) Run(ctx restate.WorkflowContext, order Order) (res OrderResult, err error) {
	orderID := restate.Key(ctx)
	status := rea.NewState[string](ctx, "status")
	res = OrderResult{OrderID: orderID, Status: "cancelled"}

	// rea's saga runs compensations inside restate.Run, so they may only talk to
	// external systems. Refunds go straight to the provider; releasing stock
	// lives in another Restate service and is sent as a durable message instead.
	saga := rea.NewSaga(ctx, "order", nil)
	saga.Register("refund_card", func(rc restate.RunContext, payload []byte) error {
		var id string
		if err := json.Unmarshal(payload, &id); err != nil {
			return err
		}
		return providerRefund(rc, id)
	})
	defer func() {
		if err != nil {
			saga.CompensateIfNeeded(&err)
			_ = status.Set("cancelled: " + err.Error())
		}
	}()

	_ = status.Set("reserving")
	if _, err = reserveStock.Call(ctx, order.SKU, ReserveRequest{OrderID: orderID, Qty: order.Qty}); err != nil {
		return res, err
	}
	defer func() {
		if err != nil {
			releaseStock.Send(ctx, order.SKU, orderID)
		}
	}()

	_ = status.Set("charging")
	if err = saga.Add("refund_card", orderID, true); err != nil {
		return res, err
	}
	receipt, err := chargeCard.Call(ctx, ChargeRequest{OrderID: orderID, Amount: order.Qty * order.Price})
	if err != nil {
		return res, err
	}

	_ = status.Set("completed")
	res.Status = "completed"
	res.PaymentID = receipt.PaymentID
	return res, nil
}

func (OrderWorkflow) Status(ctx restate.WorkflowSharedContext) (string, error) {
	return rea.NewState[string](ctx, "status").Get()
}

func main() {
	addr := os.Getenv("SHOP_ADDR")
	if addr == "" {
		addr = "0.0.0.0:9080"
	}
	srv := server.NewRestate().
		Bind(restate.Reflect(Inventory{})).
		Bind(restate.Reflect(Payments{})).
		Bind(restate.Reflect(OrderWorkflow{}))
	if err := srv.Start(context.Background(), addr); err != nil {
		slog.Error("shop exited", "err", err)
		os.Exit(1)
	}
}
