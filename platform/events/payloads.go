package events

type ProductPayload struct {
	ProductID   string            `json:"product_id"`
	SKU         string            `json:"sku"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Amount      int64             `json:"amount"`
	Currency    string            `json:"currency"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	Active      bool              `json:"active"`
}

type InventoryReservePayload struct {
	ReservationID string `json:"reservation_id"`
	OrderID       string `json:"order_id"`
	SKU           string `json:"sku"`
	WarehouseID   string `json:"warehouse_id"`
	Quantity      int64  `json:"quantity"`
	Reason        string `json:"reason,omitempty"`
}

type OrderPayload struct {
	OrderID     string      `json:"order_id"`
	CustomerID  string      `json:"customer_id"`
	Status      string      `json:"status"`
	Amount      int64       `json:"amount"`
	Currency    string      `json:"currency"`
	Items       []OrderItem `json:"items"`
	Reason      string      `json:"reason,omitempty"`
}

type OrderItem struct {
	SKU       string `json:"sku"`
	Quantity  int64  `json:"quantity"`
	UnitAmount int64 `json:"unit_amount"`
}

type PaymentPayload struct {
	PaymentID     string `json:"payment_id"`
	OrderID       string `json:"order_id"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
	Status        string `json:"status"`
	PSPReference  string `json:"psp_reference,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

type FulfillmentPayload struct {
	ShipmentID  string `json:"shipment_id"`
	OrderID     string `json:"order_id"`
	WarehouseID string `json:"warehouse_id"`
	Carrier     string `json:"carrier,omitempty"`
	Tracking    string `json:"tracking,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type NotificationPayload struct {
	Channel    string         `json:"channel"`
	To         string         `json:"to"`
	Template   string         `json:"template"`
	Data       map[string]any `json:"data"`
}

type SagaPayload struct {
	SagaID  string `json:"saga_id"`
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}
