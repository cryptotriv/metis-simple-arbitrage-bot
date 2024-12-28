package ethmarket

import (
	"math/big"

	"github.com/shopspring/decimal"
)

const (
	FEE_DENOMINATOR = 10000
)

func GetAmountOut(reserveIn *big.Int, reserveOut *big.Int, tokenIn *big.Int, feePerTenThousands int64) *big.Int {
	// fee is per 1000, so 0.3% is 3
	fee := FEE_DENOMINATOR - feePerTenThousands
	tokenInWithFee := big.NewInt(0).Mul(tokenIn, big.NewInt(fee))
	numerator := big.NewInt(0).Mul(tokenInWithFee, reserveOut)
	denominator := big.NewInt(0).Add(big.NewInt(0).Mul(big.NewInt(FEE_DENOMINATOR), reserveIn), tokenInWithFee)
	return big.NewInt(0).Div(numerator, denominator)
}

func GetAmountIn(reserveIn *big.Int, reserveOut *big.Int, tokenOut *big.Int, feePerTenThousands int64) *big.Int {
	// fee is per 1000, so 0.3% is 3
	fee := FEE_DENOMINATOR - feePerTenThousands
	numerator := big.NewInt(0).Mul(big.NewInt(0).Mul(tokenOut, big.NewInt(FEE_DENOMINATOR)), reserveIn)
	denominator := big.NewInt(0).Mul(big.NewInt(0).Sub(reserveOut, tokenOut), big.NewInt(fee))

	return big.NewInt(0).Add(big.NewInt(0).Div(numerator, denominator), big.NewInt(1))
}

func CalculateOptimalTokenIn(reserve1In *big.Int, reserve1Out *big.Int, reserve2In *big.Int, reserve2Out *big.Int, feePerTenThousands int64) decimal.Decimal {

	fee := decimal.NewFromFloat(float64(FEE_DENOMINATOR-feePerTenThousands) / float64(FEE_DENOMINATOR))
	reserve1InDec := decimal.NewFromBigInt(reserve1In, 0)
	reserve1OutDec := decimal.NewFromBigInt(reserve1Out, 0)
	reserve2InDec := decimal.NewFromBigInt(reserve2In, 0)
	reserve2OutDec := decimal.NewFromBigInt(reserve2Out, 0)

	allReservesMul := reserve1InDec.Mul(reserve1OutDec).Mul(reserve2InDec).Mul(reserve2OutDec)
	SqrtAllReservesMul := new(big.Int).Sqrt(allReservesMul.BigInt())

	term1 := fee.Mul(decimal.NewFromBigInt(SqrtAllReservesMul, 0))
	term2 := reserve1InDec.Mul(reserve2InDec)
	term3 := fee.Pow(decimal.NewFromInt(2)).Mul(reserve1OutDec).Add(fee.Mul(reserve2InDec))

	// Only calculate point 2, since point 1 will return negative (out of domain)
	point2 := term1.Sub(term2).Div(term3)

	return point2
}

func CalculateOptimalTokenInTwoFees(reserve1In *big.Int, reserve1Out *big.Int, reserve2In *big.Int, reserve2Out *big.Int, feePerTenThousandsReserve1 int64, feePerTenThousandsReserve2 int64) decimal.Decimal {

	fee1 := decimal.NewFromFloat(float64(FEE_DENOMINATOR-feePerTenThousandsReserve1) / float64(FEE_DENOMINATOR))
	fee2 := decimal.NewFromFloat(float64(FEE_DENOMINATOR-feePerTenThousandsReserve2) / float64(FEE_DENOMINATOR))

	reserve1InDec := decimal.NewFromBigInt(reserve1In, 0)
	reserve1OutDec := decimal.NewFromBigInt(reserve1Out, 0)
	reserve2InDec := decimal.NewFromBigInt(reserve2In, 0)
	reserve2OutDec := decimal.NewFromBigInt(reserve2Out, 0)

	allReservesMul := reserve1InDec.Mul(reserve1OutDec).Mul(reserve2InDec).Mul(reserve2OutDec).Mul(fee1.Pow(decimal.NewFromInt(3))).Mul(fee2)
	SqrtAllReservesMul := new(big.Int).Sqrt(allReservesMul.BigInt())

	term1 := fee1.Mul(reserve2InDec).Mul(reserve1InDec).Neg()
	term2 := decimal.NewFromBigInt(SqrtAllReservesMul, 0)
	term3 := reserve2InDec.Mul(fee1.Pow(decimal.NewFromInt(2))).Add(fee2.Mul(reserve1OutDec).Mul(fee1.Pow(decimal.NewFromInt(2))))

	point2 := term1.Add(term2).Div(term3)

	return point2
}
