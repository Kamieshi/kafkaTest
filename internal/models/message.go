package models

import "github.com/google/uuid"

type Message struct {
	ID      uuid.UUID `json:"id,omitempty"`
	PayLoad string    `json:"payLoad,omitempty" db:"pay_load"`
}
