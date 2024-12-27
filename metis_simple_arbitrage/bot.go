package metis_simple_arbitrage

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cryptotriv/raikiri/gen/FlashSwapExecutorV1"
	"github.com/cryptotriv/raikiri/gen/FlashUniswapQueryV1"
	"github.com/cryptotriv/raikiri/gen/IAgoraSwapFactory"
	"github.com/cryptotriv/raikiri/gen/IHermesBaseV1PairEvents"
	"github.com/cryptotriv/raikiri/gen/INetSwapFactory"
	"github.com/cryptotriv/raikiri/gen/IUniswapV2PairEvents"
	"github.com/cryptotriv/raikiri/gen/TokenProvidenceV1"
	"github.com/cryptotriv/raikiri/lib/botconfig"
	"github.com/cryptotriv/raikiri/lib/deployments"
	"github.com/cryptotriv/raikiri/lib/influxdb"
	"github.com/cryptotriv/raikiri/lib/models"
	"github.com/cryptotriv/raikiri/lib/telegram"
	"github.com/cryptotriv/raikiri/lib/util"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/loov/hrtime"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	MAX_DROP_THRESHOLD_ETH = 0.5
)

func RunMetisSimpleArbitrageBot(_logger *zap.Logger, _mainWg *sync.WaitGroup, stop_ch chan bool) {
	// Store arguments
	logger = _logger
	mainWg = _mainWg

	// Load configs
	allConfig, _, _ = botconfig.Get()
	config = allConfig.MetisSimpleArbitrageBot

	DEBUG = allConfig.DebugModeAll || config.DebugMode
	bannedTokenAddresses = config.BannedTokens
	PRIVATE_KEY_EXECUTOR = config.UseAccount

	// Build our context
	botContext = models.BotContext{
		Node: allConfig.NodeName,
		Main: allConfig.MainName,
		Bot:  BOT_NAME,
	}

	logger.Info("Starting bot",
		zap.String("bot", BOT_NAME),
		zap.String("node", botContext.Node),
		zap.String("main", botContext.Main),
	)

	// Init constants
	BASE_WEI = util.ToWei(config.BaseNativePricingAmount, 18)
	MIN_NATIVE_AMOUNT_WEI = util.ToWei(config.MinumumNativeAmount, 18)
	MIN_PROFIT_WEI = util.ToWei(config.MinimumProfit, 18)
	MIN_PROFIT_WEI_FOLLOWUP = util.ToWei(config.MinimumProfit/MIN_PROFIT_FOLLOWUP_DIVISOR, 18)
	STALE_RESERVE = big.NewInt(0)
	UPDATED_RESERVE = big.NewInt(1)

	// Send init message
	if DEBUG {
		logger.Info("DEBUG MODE ON")
	}

	str, err := json.Marshal(config)
	if err != nil {
		logger.Error("Error marshalling config", zap.Error(err))
		exit = true
	}

	logger.Info("Setting up...", zap.String("version", BOT_VERSION), zap.String("config", string(str)))

	if config.WriteOnlyNetworkIndex < 0 {
		RPC_URL := config.AvailableNetworks[config.ReadAndWriteNetworkIndex]
		rpc_url := os.Getenv(RPC_URL)

		// Connect to client
		var err error
		readClient, err = ethclient.Dial(rpc_url)
		if err != nil {
			logger.Error("Error connecting to client", zap.Error(err))
			exit = true
		}
		defer readClient.Close()

		// These two are same
		writeClient = readClient
	} else {
		READ_RPC_URL := config.AvailableNetworks[config.ReadAndWriteNetworkIndex]
		WRITE_RPC_URL := config.AvailableNetworks[config.WriteOnlyNetworkIndex]

		read_rpc_url := os.Getenv(READ_RPC_URL)
		write_rpc_url := os.Getenv(WRITE_RPC_URL)

		// Connect to clients
		var err error
		readClient, err = ethclient.Dial(read_rpc_url)
		if err != nil {
			logger.Error("Error connecting to client", zap.Error(err))
			exit = true
		}
		defer readClient.Close()

		// These two are same
		writeClient, err = ethclient.Dial(write_rpc_url)
		if err != nil {
			logger.Error("Error connecting to client", zap.Error(err))
			exit = true
		}
		defer writeClient.Close()
	}

	// Get DEX data
	// Get dynamic fees
	agoraSwapFactory, err := IAgoraSwapFactory.NewIAgoraSwapFactory(common.HexToAddress(AGORASWAP_FACTORY_ADDRESS), readClient)
	if err != nil {
		logger.Error("Error getting AgoraSwap factory", zap.Error(err))
		exit = true
	}
	netSwapFactory, err := INetSwapFactory.NewINetSwapFactory(common.HexToAddress(NETSWAP_FACTORY_ADDRESS), readClient)
	if err != nil {
		logger.Error("Error getting NetSwap factory", zap.Error(err))
		exit = true
	}

	agoraSwapFee, err := agoraSwapFactory.Fee(nil)
	if err != nil {
		logger.Error("Error getting AgoraSwap fee", zap.Error(err))
		exit = true
	}

	netSwapFee, err := netSwapFactory.FeeRate(nil)
	if err != nil {
		logger.Error("Error getting NetSwap fee", zap.Error(err))
		exit = true
	}

	agoraSwapFee.Mul(agoraSwapFee, big.NewInt(10))
	netSwapFee.Mul(netSwapFee, big.NewInt(10))

	logger.Info("Pulled DEX fees in FeePerTenThousands",
		zap.String("agoraSwap", agoraSwapFee.String()),
		zap.String("netSwap", netSwapFee.String()),
	)

	uniswapV2FactoryAddressFeePerTenThousands[AGORASWAP_FACTORY_ADDRESS] = agoraSwapFee.Int64()
	uniswapV2FactoryAddressFeePerTenThousands[NETSWAP_FACTORY_ADDRESS] = netSwapFee.Int64()

	// Get our contract deployments
	flashQueryAddress, err := deployments.GetDeployedContract(readClient, "FlashUniswapQueryV1")
	if err != nil {
		logger.Error("Error getting FlashUniswapQueryV1 address", zap.Error(err))
		exit = true
	}

	executorContractAddress, err := deployments.GetDeployedContract(readClient, "FlashSwapExecutorV1")
	if err != nil {
		logger.Error("Error getting FlashSwapExecutorV1 address", zap.Error(err))
		exit = true
	}

	tokenProvidenceAddress, err := deployments.GetDeployedContract(readClient, "TokenProvidenceV1")
	if err != nil {
		logger.Error("Error getting TokenProvidenceV1 address", zap.Error(err))
		exit = true
	}

	logger.Info("Contracts loaded",
		zap.String("flashUniswapQueryV1", flashQueryAddress.Hex()),
		zap.String("flashSwapExecutorV1", executorContractAddress.Hex()),
		zap.String("tokenProvidenceV1", tokenProvidenceAddress.Hex()),
	)

	// Get contract bindings
	flashQueryInstance, err := FlashUniswapQueryV1.NewFlashUniswapQueryV1(flashQueryAddress, readClient)
	if err != nil {
		logger.Error("Error getting FlashUniswapQueryV1 instance", zap.Error(err))
		exit = true
	}

	executorContract, err := FlashSwapExecutorV1.NewFlashSwapExecutorV1(executorContractAddress, readClient)
	if err != nil {
		logger.Error("Error getting FlashSwapExecutorV1 instance", zap.Error(err))
		exit = true
	}

	tokenProvidenceContract, err := TokenProvidenceV1.NewTokenProvidenceV1(tokenProvidenceAddress, readClient)
	if err != nil {
		logger.Error("Error getting TokenProvidenceV1 instance", zap.Error(err))
		exit = true
	}

	// Let's setup our executor account here
	privateKey, err := crypto.HexToECDSA(os.Getenv(PRIVATE_KEY_EXECUTOR))
	if err != nil {
		logger.Error("Error in HexToECDSA for private key", zap.Error(err))
		exit = true
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		logger.Error("cannot assert type: publicKey is not of type *ecdsa.PublicKey")
		exit = true
	}

	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	chainId, err := readClient.ChainID(context.Background())
	if err != nil {
		logger.Error("Error getting chainId", zap.Error(err))
		exit = true
	}

	nonce, err = readClient.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		logger.Error("Error getting nonce", zap.Error(err))
		exit = true
	}

	balance, err := readClient.BalanceAt(context.Background(), fromAddress, nil)
	if err != nil {
		logger.Error("Error getting balance", zap.Error(err))
		exit = true
	}

	currBalance := balance

	// Get initial gas price
	MIN_GAS_GWEI, err = readClient.SuggestGasPrice(context.Background())
	if err != nil {
		logger.Error("Error getting gas price", zap.Error(err))
		exit = true
	}

	calculateMinProfit()

	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainId)
	if err != nil {
		logger.Error("Error generating auth", zap.Error(err))
		exit = true
	}

	auth.Value = big.NewInt(0)      // in wei
	auth.GasLimit = uint64(3000000) // in units
	auth.GasPrice = big.NewInt(0).Add(MIN_GAS_GWEI, util.ToWei(MIN_GAS_GWEI_BUFFER, 9))
	auth.NoSend = false

	logger.Info("Minimum gas price: ", zap.String("gasPrice", util.ToDecimal(MIN_GAS_GWEI, 9).String()))

	// Setup transaction
	auth.Nonce = big.NewInt(int64(nonce))

	if !DEBUG {
		// Initialize all markets
		initAllMarketData(flashQueryInstance)
		updateReservesBatched(flashQueryInstance)
		filterMarkets(
			flashQueryInstance,
			tokenProvidenceContract,
			privateKey,
			chainId,
			nonce,
			MIN_GAS_GWEI,
			fromAddress,
			tokenProvidenceAddress,
			readClient)
		sortMartkets()
		mapMarketAddresses()
	} else {
		// DEBUG: Load test data from our json file
		marketPairsString, err := os.ReadFile(filepath.Join(DATA_BASEPATH, MARKET_PAIRS_BY_TOKEN_JSON_PATH))
		if err != nil {
			logger.Error("Error reading file", zap.Error(err))
			exit = true
		}

		err = json.Unmarshal(marketPairsString, &marketPairsByToken)
		if err != nil {
			logger.Error("Error unmarshalling file", zap.Error(err))
			exit = true
		}

		allMarketAddressesString, err := os.ReadFile(filepath.Join(DATA_BASEPATH, ALL_MARKET_ADDRESSES_JSON_PATH))
		if err != nil {
			logger.Error("Error reading file", zap.Error(err))
			exit = true
		}

		err = json.Unmarshal(allMarketAddressesString, &allMarketAddresses)
		if err != nil {
			logger.Error("Error unmarshalling file", zap.Error(err))
			exit = true
		}

		allMarketReservesString, err := os.ReadFile(filepath.Join(DATA_BASEPATH, ALL_MARKET_RESERVES_JSON_PATH))
		if err != nil {
			logger.Error("Error reading file", zap.Error(err))
			exit = true
		}
		err = json.Unmarshal(allMarketReservesString, &allMarketReserves)
		if err != nil {
			logger.Error("Error unmarshalling file", zap.Error(err))
			exit = true
		}

		allMarketAddressFactoriestring, err := os.ReadFile(filepath.Join(DATA_BASEPATH, ALL_MARKET_ADDRESS_FACTORIES_JSON_PATH))
		if err != nil {
			logger.Error("Error reading file", zap.Error(err))
			exit = true
		}
		err = json.Unmarshal(allMarketAddressFactoriestring, &allMarketAddressFactories)
		if err != nil {
			logger.Error("Error unmarshalling file", zap.Error(err))
			exit = true
		}

		marketMappingString, err := os.ReadFile(filepath.Join(DATA_BASEPATH, MARKET_MAPPING_JSON_PATH))
		if err != nil {
			logger.Error("Error reading file", zap.Error(err))
			exit = true
		}
		err = json.Unmarshal(marketMappingString, &marketMapping)
		if err != nil {
			logger.Error("Error unmarshalling file", zap.Error(err))
			exit = true
		}
	}

	logger.Info("Pulled all pairs", zap.Int("totalPairs", len(allMarketAddresses)))

	// Form query
	// query := ethereum.FilterQuery{
	// 	Addresses: allMarketAddresses,
	// }

	// Get contracts
	uniswapV2ABI, err = abi.JSON(strings.NewReader(string(IUniswapV2PairEvents.IUniswapV2PairEventsABI)))
	if err != nil {
		logger.Error("Error reading uniswapV2ABI", zap.Error(err))
		exit = true
	}

	hermesV1ABI, err = abi.JSON(strings.NewReader(string(IHermesBaseV1PairEvents.IHermesBaseV1PairEventsABI)))
	if err != nil {
		logger.Error("Error reading hermesV1ABI", zap.Error(err))
		exit = true
	}

	logs := make(chan types.Log, 200)

	// sub, err := writeClient.SubscribeFilterLogs(context.Background(), query, logs)
	// if err != nil {
	// 	logger.Error("Error subscribing logs", zap.Error(err))
	// 	exit = true
	// }

	uniV2EventSignature := []byte("Sync(uint112,uint112)") //
	uniV2EventHash = crypto.Keccak256Hash(uniV2EventSignature)

	hermesEventSignature := []byte("Sync(uint256,uint256)") //
	hermesEventHash = crypto.Keccak256Hash(hermesEventSignature)

	//swapEventHash := common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822")
	//feesEventHash := common.HexToHash("0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602")

	// Update reserves to latest (just so that we don't miss any events)
	time.Sleep(time.Millisecond * 500)
	updateReserves(flashQueryInstance)
	priceMarkets()

	// Store in json the whole map so we can play around with it during testing later
	err = os.MkdirAll(DATA_BASEPATH, os.ModePerm)
	if err != nil {
		logger.Error("Error creating directory", zap.Error(err))
		exit = true
	}

	marketPairsByTokenJson, err := json.Marshal(marketPairsByToken)
	if err != nil {
		logger.Error("Error marshalling data", zap.Error(err))
		exit = true
	}

	err = os.WriteFile(filepath.Join(DATA_BASEPATH, MARKET_PAIRS_BY_TOKEN_JSON_PATH), marketPairsByTokenJson, 0644)
	if err != nil {
		logger.Error("Error writing data", zap.Error(err))
		exit = true
	}

	allMarketAddressesJson, err := json.Marshal(allMarketAddresses)
	if err != nil {
		logger.Error("Error marshalling data", zap.Error(err))
		exit = true
	}

	err = os.WriteFile(filepath.Join(DATA_BASEPATH, ALL_MARKET_ADDRESSES_JSON_PATH), allMarketAddressesJson, 0644)
	if err != nil {
		logger.Error("Error writing data", zap.Error(err))
		exit = true
	}

	allMarketReservesJson, err := json.Marshal(allMarketReserves)
	if err != nil {
		logger.Error("Error marshalling data", zap.Error(err))
		exit = true
	}

	err = os.WriteFile(filepath.Join(DATA_BASEPATH, ALL_MARKET_RESERVES_JSON_PATH), allMarketReservesJson, 0644)
	if err != nil {
		logger.Error("Error writing data", zap.Error(err))
		exit = true
	}

	allMarketAddressFactoriesJson, err := json.Marshal(allMarketAddressFactories)
	if err != nil {
		logger.Error("Error marshalling data", zap.Error(err))
		exit = true
	}

	err = os.WriteFile(filepath.Join(DATA_BASEPATH, ALL_MARKET_ADDRESS_FACTORIES_JSON_PATH), allMarketAddressFactoriesJson, 0644)
	if err != nil {
		logger.Error("Error writing data", zap.Error(err))
		exit = true
	}

	marketMappingJson, err := json.Marshal(marketMapping)
	if err != nil {
		logger.Error("Error marshalling data", zap.Error(err))
		exit = true
	}

	err = os.WriteFile(filepath.Join(DATA_BASEPATH, MARKET_MAPPING_JSON_PATH), marketMappingJson, 0644)
	if err != nil {
		logger.Error("Error writing data", zap.Error(err))
		exit = true
	}

	markReservesAsStale()

	// Setup done
	logger.Info("Setup complete - listening to new events...")

	totalOpportunities := 0
	mostOpporunitiesInBlock := 0
	var start time.Duration

	ticker5m := time.NewTicker(5 * time.Minute)
	ticker1m := time.NewTicker(1 * time.Minute)
	ticker1s := time.NewTicker(1 * time.Second)

	previousBlock := uint64(0)
	prematureCalcs := 0
	subsequentEvents := 0

	highestBalance := decimal.NewFromInt(0)

	// Main loop
	for {
		// Exit if triggered
		if exit {
			stop_ch <- true
			break
		}

		// Check for stop signal
		select {
		case <-stop_ch:
			logger.Info("Stop signal received")
			exit = true
		// case err := <-sub.Err():
		// 	if err != nil {
		// 		logger.Error("Error in subscription", zap.Error(err))
		// 		err := telegram.Update(botContext, fmt.Sprint("Balance: ", util.ToDecimal(currBalance, 18).String()))
		// 		if err != nil {
		// 			logger.Error("Error updating to telegram", zap.Error(err))
		// 		}
		// 		exit = true
		// 	}
		case <-ticker1m.C:
			// Update our nonce
			nonce, err = readClient.PendingNonceAt(context.Background(), fromAddress)
			if err != nil {
				logger.Error("Error getting nonce", zap.Error(err))
				exit = true
			}

			auth.Nonce = big.NewInt(int64(nonce))

			logger.Info("Synced nonces to: ", zap.Int64("nonce", int64(nonce)))
		case <-ticker5m.C:
			// Update our balance
			currBalance, err = readClient.BalanceAt(context.Background(), fromAddress, nil)
			if err != nil {
				logger.Error("Error getting balance", zap.Error(err))
				exit = true
				continue
			}

			// Update our gas price
			MIN_GAS_GWEI, err = readClient.SuggestGasPrice(context.Background())
			if err != nil {
				logger.Error("Error getting gas price", zap.Error(err))
				exit = true
			}

			calculateMinProfit()

			auth.GasPrice = big.NewInt(0).Add(MIN_GAS_GWEI, util.ToWei(MIN_GAS_GWEI_BUFFER, 9))

			// Update influxDB
			go func(currBalance *big.Int) {
				err := telegram.Update(botContext, fmt.Sprint("Balance: ", util.ToDecimal(currBalance, 18).String()))
				if err != nil {
					logger.Error("Error updating to telegram", zap.Error(err))
				}

				ethBalanceFloat, _ := util.ToDecimal(currBalance, 18).Float64()

				// Write to influxDb
				influxdb.WriteMEVBalance(botContext, ethBalanceFloat, 0.0, 0.0)

				// Flush
				influxdb.Flush()
			}(currBalance)

			// Safety
			currBalDecimals := util.ToDecimal(currBalance, 18)
			if currBalDecimals.Cmp(highestBalance) > 0 {
				highestBalance = currBalDecimals
			}

			// Check if we had a big drop
			if highestBalance.Sub(currBalDecimals).Cmp(decimal.NewFromFloat(MAX_DROP_THRESHOLD_ETH)) > 0 {
				// Notify rika
				telegram.Notify(botContext, fmt.Sprint("Large balance drop detected - please check status. Sleeping..."))

				// Sleep for a long time
				time.Sleep(time.Hour * 24)
			}

		case <-ticker1s.C:
			updateReserves(flashQueryInstance)

			// Start time
			start = hrtime.Now()

			arbTxs := evaluateMarketsRecursiveAll(false, 0) // evaluateMarkets()

			processingDone := hrtime.Since(start)

			logger.Info("Polling and Processing Done", zap.String("duration", processingDone.String()))

			if len(arbTxs) <= 1 {
				logger.Debug("No Arbs in Processed Event Block No", zap.Uint64("blockNumber", previousBlock))
				logger.Debug("Total Time", zap.String("duration", hrtime.Since(start).String()))
			} else {
				// Actually take the opportunity
				go takeOpportunities(
					executorContract,
					executorContractAddress,
					fromAddress,
					auth,
					readClient,
					arbTxs)

				logger.Debug("Arbs in Processed Event Block No: ", zap.Uint64("blockNumber", previousBlock))

				// Optimisation: Let's do all non-critical stuff here
				// Update our nonce
				nonce++

				auth, err = bind.NewKeyedTransactorWithChainID(privateKey, chainId)
				if err != nil {
					logger.Error("Error generating auth", zap.Error(err))
					exit = true
				}

				auth.Value = big.NewInt(0)      // in wei
				auth.GasLimit = uint64(3000000) // in units
				auth.GasPrice = big.NewInt(0).Add(MIN_GAS_GWEI, util.ToWei(MIN_GAS_GWEI_BUFFER, 9))
				auth.NoSend = false

				// Setup transaction
				auth.Nonce = big.NewInt(int64(nonce))

				// Track total opportunities
				totalOpportunities++

				// Record the highest number of opportunities in block
				if len(arbTxs) > mostOpporunitiesInBlock {
					mostOpporunitiesInBlock = len(arbTxs)
				}

				// Log the opportunities
				logger.Info("There were opportunities for profit in processed block", zap.Uint64("blockNumber", previousBlock))

				for count, crossedMarket := range arbTxs {
					logger.Info(fmt.Sprintf("Opportunity %d", count),
						zap.String("size", util.ToDecimal(crossedMarket.NativeInAmount, 18).String()),
						zap.String("tokenOut", util.ToDecimal(crossedMarket.NativeOutAmount, 18).String()),
						zap.String("profit", util.ToDecimal(crossedMarket.Profit, 18).String()),
						zap.String("buyFromMarket", crossedMarket.BuyFromPair.Hex()),
						zap.String("sellToMarket", crossedMarket.SellToPair.Hex()),
					)
				}

				// Get max profit
				totalProfit := big.NewInt(0)
				for _, arb := range arbTxs {
					totalProfit.Add(totalProfit, arb.Profit)
				}

				profitFloat, _ := util.ToDecimal(totalProfit, 18).Float64()

				// Write to influxDb
				influxdb.WriteMEVOpportunity(botContext, "", 0, profitFloat)
			}

			markReservesAsStale()

			logger.Info("Update",
				zap.Int("totalOpportunities", totalOpportunities),
				zap.Int("arbTxSentCount", arbTxSentCount),
				zap.Int("failedTxs", totalOpportunities-arbTxSentCount),
				zap.String("balance", util.ToDecimal(currBalance, 18).String()),
			)

		case vLog := <-logs:

			// Get log
			if vLog.Topics[0] != uniV2EventHash && vLog.Topics[0] != hermesEventHash {
				continue
			}

			logger.Info("Got event", zap.Uint64("blockNumber", vLog.BlockNumber))

			if vLog.BlockNumber == previousBlock {
				logger.Debug("We didn't wait for all events")
				prematureCalcs++
			}

			// Set the previous block
			previousBlock = vLog.BlockNumber

			// Start time
			start = hrtime.Now()

			// Update reserves
			tokenAddr := updateReservesByEvent(vLog)

			var arbTxs []FlashSwapExecutorV1.Arb
			subsequentEventOccurred := false
			newEvent := true

			// Poll for an amount of time via spinlock
			for hrtime.Since(start) < (time.Microsecond*time.Duration(config.EventDelayMs)) || len(logs) > 0 || newEvent {
				for len(logs) > 0 {
					vLog := <-logs

					if vLog.Topics[0] != uniV2EventHash && vLog.Topics[0] != hermesEventHash {
						continue
					}

					logger.Info("Got event at block", zap.Uint64("blockNumber", vLog.BlockNumber))

					if vLog.BlockNumber > previousBlock {
						previousBlock = vLog.BlockNumber
					}

					tokenAddr = updateReservesByEvent(vLog)
					subsequentEventOccurred = true

					if !newEvent {
						newEvent = true
					}
				}

				if newEvent {
					// Evaluate all markets
					arbTxs = evaluateMarketsRecursive(false, tokenAddr, 0) // evaluateMarkets()
					newEvent = false
				}
			}

			pollingDone := hrtime.Since(start)

			logger.Info("Polling and Processing Done", zap.String("duration", pollingDone.String()))

			if len(arbTxs) <= 1 {
				logger.Debug("No Arbs in Processed Event Block No", zap.Uint64("blockNumber", previousBlock))
				logger.Debug("Total Time", zap.String("duration", hrtime.Since(start).String()))
			} else {
				// Actually take the opportunity
				go takeOpportunities(
					executorContract,
					executorContractAddress,
					fromAddress,
					auth,
					readClient,
					arbTxs)

				logger.Debug("Arbs in Processed Event Block No: ", zap.Uint64("blockNumber", previousBlock))

				// Optimisation: Let's do all non-critical stuff here
				// Update our nonce
				nonce++

				auth, err = bind.NewKeyedTransactorWithChainID(privateKey, chainId)
				if err != nil {
					logger.Error("Error generating auth", zap.Error(err))
					exit = true
				}

				auth.Value = big.NewInt(0)      // in wei
				auth.GasLimit = uint64(3000000) // in units
				auth.GasPrice = big.NewInt(0).Add(MIN_GAS_GWEI, util.ToWei(MIN_GAS_GWEI_BUFFER, 9))
				auth.NoSend = false

				// Setup transaction
				auth.Nonce = big.NewInt(int64(nonce))

				// Track total opportunities
				totalOpportunities++

				// Record the highest number of opportunities in block
				if len(arbTxs) > mostOpporunitiesInBlock {
					mostOpporunitiesInBlock = len(arbTxs)
				}

				// Log the opportunities
				logger.Info("There were opportunities for profit in processed block", zap.Uint64("blockNumber", previousBlock))

				for count, crossedMarket := range arbTxs {
					logger.Info(fmt.Sprintf("Opportunity %d", count),
						zap.String("size", util.ToDecimal(crossedMarket.NativeInAmount, 18).String()),
						zap.String("tokenOut", util.ToDecimal(crossedMarket.NativeOutAmount, 18).String()),
						zap.String("profit", util.ToDecimal(crossedMarket.Profit, 18).String()),
						zap.String("buyFromMarket", crossedMarket.BuyFromPair.Hex()),
						zap.String("sellToMarket", crossedMarket.SellToPair.Hex()),
					)
				}

				// Get max profit
				totalProfit := big.NewInt(0)
				for _, arb := range arbTxs {
					totalProfit.Add(totalProfit, arb.Profit)
				}

				profitFloat, _ := util.ToDecimal(totalProfit, 18).Float64()

				// Write to influxDb
				influxdb.WriteMEVOpportunity(botContext, vLog.TxHash.Hex(), int(vLog.BlockNumber), profitFloat)
			}

			markReservesAsStale()

			if subsequentEventOccurred {
				subsequentEvents++
			}

			logger.Info("Update",
				zap.Int("totalOpportunities", totalOpportunities),
				zap.Int("arbTxSentCount", arbTxSentCount),
				zap.Int("failedTxs", totalOpportunities-arbTxSentCount),
				zap.String("balance", util.ToDecimal(currBalance, 18).String()),
			)
		}
	}

	// Let's summarize our session here
	currBalance, err = readClient.BalanceAt(context.Background(), fromAddress, nil)
	if err != nil {
		logger.Error("Error getting balance", zap.Error(err))
		exit = true
	}

	// Cleanup here after exit
	logger.Info("Cleanup...")
	influxdb.Flush()

	mainWg.Done()
}
