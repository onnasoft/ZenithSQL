package validate

import (
	"fmt"
	"math/big"
)

type IsDivisibleBy struct {
	Divisor string
}

func (v IsDivisibleBy) Validate(value interface{}, colName string) error {
	valStr, ok := value.(string)
	if !ok {
		return fmt.Errorf("column '%s' must be a string for IsDivisibleBy validation", colName)
	}
	val := new(big.Int)
	div := new(big.Int)
	if _, ok := val.SetString(valStr, 10); !ok {
		return fmt.Errorf("column '%s' value is not a valid integer", colName)
	}
	if _, ok := div.SetString(v.Divisor, 10); !ok {
		return fmt.Errorf("invalid divisor: %s", v.Divisor)
	}
	if new(big.Int).Mod(val, div).Cmp(big.NewInt(0)) != 0 {
		return fmt.Errorf("column '%s' value must be divisible by %s", colName, v.Divisor)
	}
	return nil
}
