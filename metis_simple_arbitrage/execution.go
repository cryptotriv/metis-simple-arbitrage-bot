package metis_simple_arbitrage

import (
	"strings"
	"time"

	"github.com/cryptotriv/raikiri/gen/FlashSwapExecutorV1"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/loov/hrtime"
	"go.uber.org/zap"
)

func takeOpportunities(
	executorContract *FlashSwapExecutorV1.FlashSwapExecutorV1,
	executorContractAddress common.Address,
	fromAddress common.Address,
	auth *bind.TransactOpts,
	// privateKey *ecdsa.PrivateKey,
	// chainId *big.Int,
	// currentNonce uint64,
	// gasPrice *big.Int,
	readClient *ethclient.Client,
	arbs []FlashSwapExecutorV1.Arb) {

	var start time.Duration

	if !config.PerformanceMode {
		start = hrtime.Now()
	}

	// Build transaction
	// if config.SimulateTxs {

	// 	auth.NoSend = true
	// 	simulateTx, err := executorContract.ExecuteNativeArb(
	// 		auth,
	// 		arbs,
	// 		MIN_PROFIT_WEI_FOLLOWUP)
	// 	if handleIf(err) {
	// 		return
	// 	}

	// 	// Estimate gas used
	// 	msg := ethereum.CallMsg{
	// 		From:     fromAddress,
	// 		To:       &executorContractAddress,
	// 		Gas:      simulateTx.Gas(),
	// 		GasPrice: simulateTx.GasPrice(),
	// 		Value:    simulateTx.Value(),
	// 		Data:     simulateTx.Data(),
	// 	}

	// 	_, err = readClient.EstimateGas(context.Background(), msg)
	// 	if handleIf(err) {
	// 		logMessage(models.Error, "EstimateGas failed for arb, skipping")
	// 		return
	// 	}

	// 	if !config.PerformanceMode {
	// 		logMessage(models.Info, fmt.Sprint("Simulate arb done: ", hrtime.Since(start)))
	// 	}

	// 	auth.NoSend = false
	// }

	// Send transaction
	tx, err := executorContract.ExecuteNativeArb(
		auth,
		arbs,
		MIN_PROFIT_WEI_FOLLOWUP)

	logger.Debug("Sent arb tx with nonce: ", zap.Uint64("nonce", auth.Nonce.Uint64()))
	logger.Debug("Tx for arb sent: ", zap.String("duration", hrtime.Since(start).String()))

	if err == nil {
		logger.Info("Arb Tx Sent! Hash: ", zap.String("hash", tx.Hash().Hex()))

		mu.Lock()
		arbTxSentCount++
		mu.Unlock()

		// time.Sleep(30 * time.Second)

		// Get the receipt to see if the tx was successful
		// receipt, err := readClient.TransactionReceipt(context.Background(), tx.Hash())
		// if err != nil {
		// 	logger.Error("Error getting receipt for arb tx: ", zap.Error(err))
		// 	return
		// }

		// revert := false
		// if receipt.Status == 0 {
		// 	revert = true
		// }

		// success := false
		// if len(receipt.Logs) > 0 {
		// 	success = true
		// }

		// influxdb.WriteMEVTxSent(botContext, tx.Hash().Hex(), int(receipt.BlockNumber.Int64()), success, revert)
	} else if strings.Contains(err.Error(), "nonce too low") {
		logger.Error("Expected error found for arb tx: ", zap.Error(err))
		logger.Error("Another bot sent a faster tx for arb")
	} else {
		// This error we are not sure, let's log it
		logger.Error("Unhandled error found for arb tx: ", zap.Error(err))
	}
}
