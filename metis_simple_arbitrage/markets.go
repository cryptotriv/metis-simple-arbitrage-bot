package metis_simple_arbitrage

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"sort"
	"time"

	"github.com/cryptotriv/raikiri/gen/FlashSwapExecutorV1"
	"github.com/cryptotriv/raikiri/gen/FlashUniswapQueryV1"
	"github.com/cryptotriv/raikiri/gen/TokenProvidenceV1"
	"github.com/cryptotriv/raikiri/lib/ethmarket"
	"github.com/cryptotriv/raikiri/lib/models"
	"github.com/cryptotriv/raikiri/lib/util"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/loov/hrtime"
	"go.uber.org/zap"
)

func initAllMarketData(flashQueryInstance *FlashUniswapQueryV1.FlashUniswapQueryV1) {
	// Repeat for each factory address
	for _, factoryAddress := range uniswapV2FactoryAddresses {
		logger.Info("Querying for Factory Address: ", zap.String("factoryAddress", factoryAddress))
		totalPairs := 0

		// Repeat for multiple batches, call getPairsByIndexRange
		for count := 0; count < BATCH_COUNT_LIMIT*UNISWAP_BATCH_SIZE; count += UNISWAP_BATCH_SIZE {
			batch, err := flashQueryInstance.GetPairsByIndexRange(nil, common.HexToAddress(factoryAddress), big.NewInt(int64(count)), big.NewInt(int64(count+UNISWAP_BATCH_SIZE)))
			if err != nil {
				logger.Error("Error querying for pairs", zap.Error(err))
				exit = true
			}

			totalPairs += len(batch)

			var addressFilter []common.Address
			var pairIsStable []bool

			// Prepare to filter batch if these are Hermes pairs
			if factoryAddress == HERMES_FACTORY_ADDRESS {
				for _, pair := range batch {
					addressFilter = append(addressFilter, pair[2])
				}
				pairIsStable, err = flashQueryInstance.FilterVolatileHermesPairs(nil, addressFilter)
				if err != nil {
					logger.Error("Error querying for Hermes pairs", zap.Error(err))
					exit = true
				}
			}

			for index, pair := range batch {
				metisIndex, tokenIndex := getTokenIndexesInPair(pair[0], pair[1])
				if metisIndex == -1 {
					continue // We ignore non-Metis pairs for now
				}

				tokenAddress := pair[tokenIndex]
				metisAddress := pair[metisIndex]

				// Check if token is banned
				if addressInSlice(tokenAddress, bannedTokenAddresses) {
					continue
				}

				// If HERMES pair, check if it's stable or not
				if factoryAddress == HERMES_FACTORY_ADDRESS && pairIsStable[index] {
					continue
				}

				// Hard ban
				if pair[2] == common.HexToAddress("0x7b934F9d64FCEA42967DB7e5Fb15F2dBEe95Db24") {
					continue
				}

				// Call reserves to see if it reverts
				// _, err := flashQueryInstance.GetReservesByPairs(nil, []common.Address{pair[2]})
				// if err != nil {
				// 	logger.Error("Error querying for reserves", zap.String("address", pair[2].Hex()), zap.Error(err))
				// 	continue
				// }

				// Add to market
				uniswapV2Pair := models.UniswappyV2Pair{
					MarketAdress:       pair[2],
					Factory:            common.HexToAddress(factoryAddress),
					FeePerTenThousands: uniswapV2FactoryAddressFeePerTenThousands[factoryAddress],
					TokenAddresses:     [2]common.Address{pair[0], pair[1]},
					WethAddress:        metisAddress,
					NativeIndex:        metisIndex,
					TokenIndex:         tokenIndex}

				marketPairsByToken[tokenAddress] = append(marketPairsByToken[tokenAddress], uniswapV2Pair)
				allMarketAddresses = append(allMarketAddresses, pair[2])
			}

			if len(batch) < UNISWAP_BATCH_SIZE {
				break // We have queried all available markets for this factory address
			}
		}

		logger.Info("Total pairs for the factory address: ", zap.Int("totalPairs", totalPairs))
	}
}

func updateReserves(flashQueryInstance *FlashUniswapQueryV1.FlashUniswapQueryV1) {
	var start time.Duration

	if !config.PerformanceMode {
		start = hrtime.Now()
	}

	// We update reserves
	var err error
	allMarketReserves, err = flashQueryInstance.GetReservesByPairs(nil, allMarketAddresses)
	if err != nil {
		logger.Error("Error querying for reserves", zap.Error(err))
		exit = true
	}

	logger.Info("Update All Reserves", zap.String("duration", hrtime.Since(start).String()))
}

func updateReservesBatched(flashQueryInstance *FlashUniswapQueryV1.FlashUniswapQueryV1) {
	var start time.Duration

	if !config.PerformanceMode {
		start = hrtime.Now()
	}

	// We update reserves
	batches := len(allMarketAddresses) / 200
	modulus := len(allMarketAddresses) % 200

	logger.Info("Update All Reserves", zap.Int("length", len(allMarketAddresses)), zap.Int("batches", batches), zap.Int("remainder", modulus))

	for count := 0; count < batches+1; count++ {
		var length int

		if count == batches {
			length = modulus
		} else {
			length = 200
		}

		marketReserves, err := flashQueryInstance.GetReservesByPairs(nil, allMarketAddresses[count*200:(count*200)+length])
		if err != nil {
			logger.Error("Error querying for batched reserves", zap.Error(err))
			exit = true
		}

		allMarketReserves = append(allMarketReserves, marketReserves...)
	}

	logger.Info("Update All Reserves", zap.String("duration", hrtime.Since(start).String()))
}

// Only called when event IsBlock is false
func updateReservesByEvent(vLog types.Log) common.Address {

	// Unpack accordingly
	if vLog.Topics[0] == hermesEventHash {
		err := hermesV1ABI.UnpackIntoInterface(&reservesUpdate, "Sync", vLog.Data)
		if err != nil {
			logger.Error("Error unpacking hermesEventHash", zap.Error(err))
			exit = true
		}
	} else {
		err := uniswapV2ABI.UnpackIntoInterface(&reservesUpdate, "Sync", vLog.Data)
		if err != nil {
			logger.Error("Error unpacking uniswapV2", zap.Error(err))
			exit = true
		}
	}

	mapping := marketMapping[vLog.Address]
	pair := marketPairsByToken[mapping.TokenAddress][mapping.Index]

	// Update reserves
	allMarketReserves[pair.TokenReserveIndex][0] = new(big.Int).Set(reservesUpdate.Reserve0)
	allMarketReserves[pair.TokenReserveIndex][1] = new(big.Int).Set(reservesUpdate.Reserve1)
	allMarketReserves[pair.TokenReserveIndex][2] = UPDATED_RESERVE

	// Update prices
	// How much token will I get from BASE_WEI Metis
	marketPairsByToken[mapping.TokenAddress][mapping.Index].SellWethPrice = ethmarket.GetAmountOut(allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
		allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
		BASE_WEI,
		pair.FeePerTenThousands)

	// How much token do I need to buy back BASE_WEI Metis
	marketPairsByToken[mapping.TokenAddress][mapping.Index].BuyWethPrice = ethmarket.GetAmountIn(allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
		allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
		BASE_WEI,
		pair.FeePerTenThousands)

	return mapping.TokenAddress
}

func filterMarkets(
	flashQueryInstance *FlashUniswapQueryV1.FlashUniswapQueryV1,
	tokenProvidenceContract *TokenProvidenceV1.TokenProvidenceV1,
	privateKey *ecdsa.PrivateKey,
	chainId *big.Int,
	nonce uint64,
	gasPrice *big.Int,
	fromAddress common.Address,
	tokenProvidenceAddress common.Address,
	readClient *ethclient.Client) {
	// Here, we repeat through all pairs in marketPairsByToken, find their position in allMarketAddresses and assign an index
	for token, pairs := range marketPairsByToken {
		for pairCount, pair := range pairs {
			for marketIndex, marketAddress := range allMarketAddresses {
				if pair.MarketAdress == marketAddress {
					marketPairsByToken[token][pairCount].TokenReserveIndex = marketIndex
					break
				}
			}
		}
	}

	var newMarketPairsByTokenWithMinAmounts map[common.Address][]models.UniswappyV2Pair = make(map[common.Address][]models.UniswappyV2Pair)

	// Make sure Metis reserve greater than minimum
	for token, pairs := range marketPairsByToken {
		for _, pair := range pairs {
			metisReserve := allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex]
			tokenReserve := allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex]

			if metisReserve.Cmp(MIN_NATIVE_AMOUNT_WEI) >= 0 && tokenReserve.Cmp(big.NewInt(100)) >= 0 {
				newMarketPairsByTokenWithMinAmounts[token] = append(newMarketPairsByTokenWithMinAmounts[token], pair)
			}
		}
	}

	// Remove all tokens that don't have multiple markets
	var newMarketPairsByToken map[common.Address][]models.UniswappyV2Pair = make(map[common.Address][]models.UniswappyV2Pair)

	for token, pairs := range newMarketPairsByTokenWithMinAmounts {
		if len(pairs) > 1 {
			newMarketPairsByToken[token] = append(newMarketPairsByToken[token], pairs...)
		}
	}

	// Do health check for each token
	var newAllMarketAddresses []common.Address
	var newHealthyMarketPairsByToken map[common.Address][]models.UniswappyV2Pair = make(map[common.Address][]models.UniswappyV2Pair)
	var newAllMarketAddressFactories []common.Address

	for token, pairs := range newMarketPairsByToken {
		if tokenIsHealthy(token,
			pairs[0],
			tokenProvidenceContract,
			privateKey,
			chainId,
			nonce,
			gasPrice,
			fromAddress,
			tokenProvidenceAddress,
			readClient) {
			newHealthyMarketPairsByToken[token] = append(newHealthyMarketPairsByToken[token], pairs...)

			for _, pair := range pairs {
				newAllMarketAddresses = append(newAllMarketAddresses, pair.MarketAdress)
				newAllMarketAddressFactories = append(newAllMarketAddressFactories, pair.Factory)
			}
		}
	}

	// Assign new to global vars
	marketPairsByToken = newHealthyMarketPairsByToken
	allMarketAddresses = newAllMarketAddresses
	allMarketAddressFactories = newAllMarketAddressFactories

	// Here we update reserves again for our new allMarketAddresses
	updateReserves(flashQueryInstance)

	// Here, we repeat through all pairs in marketPairsByToken, find their position in allMarketAddresses and assign an index
	// We do this again since markets have been filtered out
	for token, pairs := range marketPairsByToken {
		for pairCount, pair := range pairs {
			for marketIndex, marketAddress := range allMarketAddresses {
				if pair.MarketAdress == marketAddress {
					marketPairsByToken[token][pairCount].TokenReserveIndex = marketIndex
					break
				}
			}
		}
	}
}

func tokenIsHealthy(
	token common.Address,
	pair models.UniswappyV2Pair,
	tokenProvidenceContract *TokenProvidenceV1.TokenProvidenceV1,
	privateKey *ecdsa.PrivateKey,
	chainId *big.Int,
	nonce uint64,
	gasPrice *big.Int,
	fromAddress common.Address,
	tokenProvidenceAddress common.Address,
	readClient *ethclient.Client) bool {

	// Setup transaction
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainId)
	if err != nil {
		logger.Error("Error creating auth", zap.Error(err))
		exit = true
	}

	auth.Nonce = big.NewInt(int64(nonce))
	auth.Value = util.ToWei(0.1, 18) // in wei
	auth.GasLimit = uint64(3000000)  // in units
	auth.GasPrice = gasPrice
	auth.NoSend = true

	// Build transaction
	simulateTx, err := tokenProvidenceContract.HealthCheck(
		auth,
		pair.MarketAdress,
		token,
		big.NewInt(pair.FeePerTenThousands))

	if err != nil {
		logger.Info("Failed to generate simmulation tx", zap.Error(err))
		return false
	}

	// Estimate gas used
	msg := ethereum.CallMsg{
		From:     fromAddress,
		To:       &tokenProvidenceAddress,
		Gas:      simulateTx.Gas(),
		GasPrice: simulateTx.GasPrice(),
		Value:    simulateTx.Value(),
		Data:     simulateTx.Data(),
	}

	// Simulate transaction
	_, err = readClient.CallContract(context.Background(), msg, nil)
	if err != nil {
		logger.Info("Token is unhealthy - removed from markets", zap.String("tokenAddress", token.Hex()), zap.Error(err))
		return false
	}

	return true
}

func calculateMinProfit() {
	txCost := big.NewInt(0).Mul(big.NewInt(0).Add(MIN_GAS_GWEI, util.ToWei(MIN_GAS_GWEI_BUFFER, 9)), big.NewInt(ARB_FAILURE_GAS_COST))
	MIN_PROFIT_WEI = txCost.Mul(txCost, big.NewInt(FAILURE_BUFFER_MULTIPLIER))
	MIN_PROFIT_WEI_FOLLOWUP = big.NewInt(0).Div(MIN_PROFIT_WEI, big.NewInt(MIN_PROFIT_FOLLOWUP_DIVISOR))

	logger.Info("Min profit: ", zap.String("minProfit", util.ToDecimal(MIN_PROFIT_WEI, 18).String()))
	logger.Info("Min follow-up profit: ", zap.String("minProfitFollowUp", util.ToDecimal(MIN_PROFIT_WEI_FOLLOWUP, 18).String()))
}

func evaluateMarketsRecursive(isFollowUp bool, tokenAddress common.Address, depth int) []FlashSwapExecutorV1.Arb {
	// If we're updating via Sync events, we already priced it in updateReservesByEvent
	// Now cross all of them and find those with profit potential
	var arbs []FlashSwapExecutorV1.Arb
	var arbsCrossedMarkets [][2]models.UniswappyV2Pair

	// start := hrtime.Now()

	var crossedMarkets [][2]models.UniswappyV2Pair

	for _, refPair := range marketPairsByToken[tokenAddress] {
		for _, pair := range marketPairsByToken[tokenAddress] {
			if isStaleReserves(refPair, pair) {
				continue
			} else if refPair.MarketAdress == pair.MarketAdress {
				continue
			} else if pair.SellWethPrice.Cmp(refPair.BuyWethPrice) > 0 {
				crossedMarkets = append(crossedMarkets, [2]models.UniswappyV2Pair{refPair, pair})
			}
		}
	}

	if len(crossedMarkets) > 0 {
		var bestArb FlashSwapExecutorV1.Arb
		var bestArbCrossedMarkets [2]models.UniswappyV2Pair
		var bestProfit *big.Int
		profitOpportunityFound := false

		for i := 0; i < len(crossedMarkets); i++ {
			// Find the optimal size for the crossed market
			optimalSize := ethmarket.CalculateOptimalTokenInTwoFees(
				allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].NativeIndex],
				allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].TokenIndex],
				allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].TokenIndex],
				allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].NativeIndex],
				crossedMarkets[i][1].FeePerTenThousands,
				crossedMarkets[i][0].FeePerTenThousands).BigInt()

			// Calculate the profit from this optimal size
			tokensOutFromBuyingSize := ethmarket.GetAmountOut(
				allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].NativeIndex],
				allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].TokenIndex],
				optimalSize,
				crossedMarkets[i][1].FeePerTenThousands)

			proceedsFromSellingTokens := ethmarket.GetAmountOut(
				allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].TokenIndex],
				allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].NativeIndex],
				tokensOutFromBuyingSize,
				crossedMarkets[i][0].FeePerTenThousands)

			profit := new(big.Int).Sub(proceedsFromSellingTokens, optimalSize)

			// Store in bestCrossedMarket the highest profit
			// Or if it's the first market and passes minimum profit
			if (profit.Cmp(MIN_PROFIT_WEI) > 0 && !isFollowUp) || (profit.Cmp(MIN_PROFIT_WEI_FOLLOWUP) > 0 && isFollowUp) {
				if !profitOpportunityFound {
					profitOpportunityFound = true

					bestArb = FlashSwapExecutorV1.Arb{
						BuyFromPair:     crossedMarkets[i][1].MarketAdress,
						NativeInAmount:  optimalSize,
						TokenAmount:     tokensOutFromBuyingSize,
						NativeOutAmount: proceedsFromSellingTokens,
						SellToPair:      crossedMarkets[i][0].MarketAdress,
						Profit:          profit,
						// BuyReserve:      [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][1])},
						// SellReserve:     [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][1])},
						BuyFromFee:      uint8(crossedMarkets[i][1].FeePerTenThousands),
						SellToFee:       uint8(crossedMarkets[i][0].FeePerTenThousands),
						BuyFromIsWMetis: crossedMarkets[i][1].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
						SellToIsWMetis:  crossedMarkets[i][0].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
					}

					bestArbCrossedMarkets = crossedMarkets[i]
					// bestProfit = profit

					// Only need to find first arb
					break

				} else if profit.Cmp(bestProfit) > 0 {

					bestArb = FlashSwapExecutorV1.Arb{
						BuyFromPair:     crossedMarkets[i][1].MarketAdress,
						NativeInAmount:  optimalSize,
						TokenAmount:     tokensOutFromBuyingSize,
						NativeOutAmount: proceedsFromSellingTokens,
						SellToPair:      crossedMarkets[i][0].MarketAdress,
						Profit:          profit,
						// BuyReserve:      [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][1])},
						// SellReserve:     [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][1])},
						BuyFromFee:      uint8(crossedMarkets[i][1].FeePerTenThousands),
						SellToFee:       uint8(crossedMarkets[i][0].FeePerTenThousands),
						BuyFromIsWMetis: crossedMarkets[i][1].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
						SellToIsWMetis:  crossedMarkets[i][0].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
					}

					bestArbCrossedMarkets = crossedMarkets[i]
					bestProfit = profit
				}
			}
		}

		if profitOpportunityFound {
			arbs = append(arbs, bestArb)
			arbsCrossedMarkets = append(arbsCrossedMarkets, bestArbCrossedMarkets)
		}
	}

	// if !config.PerformanceMode {
	// 	logMessage(models.Info, fmt.Sprint("Evaluate Market Processing: ", hrtime.Since(start)))
	// }

	// Only search follow up depth of 1
	if len(arbs) > 0 && depth < 100 { //&& !isFollowUp {
		// start = hrtime.Now()

		for i := 0; i < len(arbs); i++ {
			// Update reserves based on the bestArb
			updateReserveByArb(arbs[i], arbsCrossedMarkets[i], false)
		}

		// Call this function again, and append if there is a follow-up arb
		// Due to recursive function, it will keep calling until there are no more new arbs
		followUpArbs := evaluateMarketsRecursive(true, tokenAddress, depth+1)

		for i := 0; i < len(arbs); i++ {
			// Revert the calculation from bestArb
			updateReserveByArb(arbs[i], arbsCrossedMarkets[i], true)
		}

		if len(followUpArbs) > 0 {
			// logMessage(models.Opportunity, "There is a follow-up arb!")
			arbs = append(arbs, followUpArbs...)
		}

		// if !config.PerformanceMode {
		// 	logMessage(models.Info, fmt.Sprint("Follow-Up Processing: ", hrtime.Since(start)))
		// }
	}

	return arbs
}

func evaluateMarketsRecursiveAll(isFollowUp bool, depth int) []FlashSwapExecutorV1.Arb {
	// If we're updating via Sync events, we already priced it in updateReservesByEvent
	// Now cross all of them and find those with profit potential
	var arbs []FlashSwapExecutorV1.Arb
	var arbsCrossedMarkets [][2]models.UniswappyV2Pair

	// start := hrtime.Now()

	for _, pairs := range marketPairsByToken {
		var crossedMarkets [][2]models.UniswappyV2Pair

		for _, refPair := range pairs {
			for _, pair := range pairs {
				if isStaleReserves(refPair, pair) {
					continue
				} else if refPair.MarketAdress == pair.MarketAdress {
					continue
				} else if pair.SellWethPrice.Cmp(refPair.BuyWethPrice) > 0 {
					crossedMarkets = append(crossedMarkets, [2]models.UniswappyV2Pair{refPair, pair})
				}
			}
		}

		if len(crossedMarkets) > 0 {
			var bestArb FlashSwapExecutorV1.Arb
			var bestArbCrossedMarkets [2]models.UniswappyV2Pair
			var bestProfit *big.Int
			profitOpportunityFound := false

			for i := 0; i < len(crossedMarkets); i++ {
				// Find the optimal size for the crossed market
				optimalSize := ethmarket.CalculateOptimalTokenInTwoFees(
					allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].NativeIndex],
					allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].TokenIndex],
					allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].TokenIndex],
					allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].NativeIndex],
					crossedMarkets[i][1].FeePerTenThousands,
					crossedMarkets[i][0].FeePerTenThousands).BigInt()

				// Calculate the profit from this optimal size
				tokensOutFromBuyingSize := ethmarket.GetAmountOut(
					allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].NativeIndex],
					allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][crossedMarkets[i][1].TokenIndex],
					optimalSize,
					crossedMarkets[i][1].FeePerTenThousands)

				proceedsFromSellingTokens := ethmarket.GetAmountOut(
					allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].TokenIndex],
					allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][crossedMarkets[i][0].NativeIndex],
					tokensOutFromBuyingSize,
					crossedMarkets[i][0].FeePerTenThousands)

				profit := new(big.Int).Sub(proceedsFromSellingTokens, optimalSize)

				// Store in bestCrossedMarket the highest profit
				// Or if it's the first market and passes minimum profit
				if (profit.Cmp(MIN_PROFIT_WEI) > 0 && !isFollowUp) || (profit.Cmp(MIN_PROFIT_WEI_FOLLOWUP) > 0 && isFollowUp) {
					if !profitOpportunityFound {
						profitOpportunityFound = true

						bestArb = FlashSwapExecutorV1.Arb{
							BuyFromPair:     crossedMarkets[i][1].MarketAdress,
							NativeInAmount:  optimalSize,
							TokenAmount:     tokensOutFromBuyingSize,
							NativeOutAmount: proceedsFromSellingTokens,
							SellToPair:      crossedMarkets[i][0].MarketAdress,
							Profit:          profit,
							// BuyReserve:      [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][1])},
							// SellReserve:     [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][1])},
							BuyFromFee:      uint8(crossedMarkets[i][1].FeePerTenThousands),
							SellToFee:       uint8(crossedMarkets[i][0].FeePerTenThousands),
							BuyFromIsWMetis: crossedMarkets[i][1].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
							SellToIsWMetis:  crossedMarkets[i][0].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
						}

						bestArbCrossedMarkets = crossedMarkets[i]
						// bestProfit = profit

						// Only need to find first arb
						break

					} else if profit.Cmp(bestProfit) > 0 {

						bestArb = FlashSwapExecutorV1.Arb{
							BuyFromPair:     crossedMarkets[i][1].MarketAdress,
							NativeInAmount:  optimalSize,
							TokenAmount:     tokensOutFromBuyingSize,
							NativeOutAmount: proceedsFromSellingTokens,
							SellToPair:      crossedMarkets[i][0].MarketAdress,
							Profit:          profit,
							// BuyReserve:      [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][1].TokenReserveIndex][1])},
							// SellReserve:     [2]*big.Int{new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][0]), new(big.Int).Set(allMarketReserves[crossedMarkets[i][0].TokenReserveIndex][1])},
							BuyFromFee:      uint8(crossedMarkets[i][1].FeePerTenThousands),
							SellToFee:       uint8(crossedMarkets[i][0].FeePerTenThousands),
							BuyFromIsWMetis: crossedMarkets[i][1].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
							SellToIsWMetis:  crossedMarkets[i][0].WethAddress == common.HexToAddress(WMETIS_TOKEN_ADDRESS),
						}

						bestArbCrossedMarkets = crossedMarkets[i]
						bestProfit = profit
					}
				}
			}

			if profitOpportunityFound {
				arbs = append(arbs, bestArb)
				arbsCrossedMarkets = append(arbsCrossedMarkets, bestArbCrossedMarkets)
			}
		}
	}

	// if !config.PerformanceMode {
	// 	logMessage(models.Info, fmt.Sprint("Evaluate Market Processing: ", hrtime.Since(start)))
	// }

	// Only search follow up depth of 1
	if len(arbs) > 0 && depth < 100 { //&& !isFollowUp {
		// start = hrtime.Now()

		for i := 0; i < len(arbs); i++ {
			// Update reserves based on the bestArb
			updateReserveByArb(arbs[i], arbsCrossedMarkets[i], false)
		}

		// Call this function again, and append if there is a follow-up arb
		// Due to recursive function, it will keep calling until there are no more new arbs
		followUpArbs := evaluateMarketsRecursiveAll(true, depth+1)

		for i := 0; i < len(arbs); i++ {
			// Revert the calculation from bestArb
			updateReserveByArb(arbs[i], arbsCrossedMarkets[i], true)
		}

		if len(followUpArbs) > 0 {
			// logMessage(models.Opportunity, "There is a follow-up arb!")
			arbs = append(arbs, followUpArbs...)
		}

		// if !config.PerformanceMode {
		// 	logMessage(models.Info, fmt.Sprint("Follow-Up Processing: ", hrtime.Since(start)))
		// }
	}

	return arbs
}

func updateReserveByArb(arb FlashSwapExecutorV1.Arb, crossedMarket [2]models.UniswappyV2Pair, isUndo bool) {
	// BuyFromPair: crossedMarket[1]
	// SellToPair: crossedMarket[0]

	// Update BuyFromPair reserves
	for count, pair := range marketPairsByToken[crossedMarket[1].TokenAddresses[crossedMarket[1].TokenIndex]] {
		if pair.MarketAdress == crossedMarket[1].MarketAdress {
			// Update reserves
			// Since we buy token from them, Native is added and Token is removed
			// The inverse is done on Undo
			if !isUndo {
				if crossedMarket[1].Factory.Hex() == HERMES_FACTORY_ADDRESS {
					allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex] = allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex].Add(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex], arb.NativeInAmount).Sub(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex], new(big.Int).Div(arb.NativeInAmount, big.NewInt(10000-uniswapV2FactoryAddressFeePerTenThousands[crossedMarket[1].TokenAddresses[crossedMarket[1].TokenIndex].Hex()])))
				} else {
					allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex] = allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex].Add(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex], arb.NativeInAmount)
				}
				allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].TokenIndex] = allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].TokenIndex].Sub(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].TokenIndex], arb.TokenAmount)
				allMarketReserves[crossedMarket[1].TokenReserveIndex][2] = UPDATED_RESERVE
			} else {
				if crossedMarket[1].Factory.Hex() == HERMES_FACTORY_ADDRESS {
					allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex] = allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex].Add(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex], new(big.Int).Div(arb.NativeInAmount, big.NewInt(10000-uniswapV2FactoryAddressFeePerTenThousands[crossedMarket[1].TokenAddresses[crossedMarket[1].TokenIndex].Hex()]))).Sub(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex], arb.NativeInAmount)
				} else {
					allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex] = allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex].Sub(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].NativeIndex], arb.NativeInAmount)
				}
				allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].TokenIndex] = allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].TokenIndex].Add(allMarketReserves[crossedMarket[1].TokenReserveIndex][crossedMarket[1].TokenIndex], arb.TokenAmount)
				allMarketReserves[crossedMarket[1].TokenReserveIndex][2] = UPDATED_RESERVE
			}

			// Update prices
			// How much token will I get from BASE_WEI Metis
			marketPairsByToken[crossedMarket[1].TokenAddresses[crossedMarket[1].TokenIndex]][count].SellWethPrice = ethmarket.GetAmountOut(allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
				allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
				BASE_WEI,
				pair.FeePerTenThousands)

			// How much token do I need to buy back BASE_WEI Metis
			marketPairsByToken[crossedMarket[1].TokenAddresses[crossedMarket[1].TokenIndex]][count].BuyWethPrice = ethmarket.GetAmountIn(allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
				allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
				BASE_WEI,
				pair.FeePerTenThousands)

			break
		}
	}

	// Update SellToPair reserves
	for count, pair := range marketPairsByToken[crossedMarket[0].TokenAddresses[crossedMarket[0].TokenIndex]] {
		if pair.MarketAdress == crossedMarket[0].MarketAdress {
			// Update reserves
			// Since we sell token to them, Native is removed and Token is added
			// The inverse is done on Undo
			if !isUndo {
				allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].NativeIndex] = allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].NativeIndex].Sub(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].NativeIndex], arb.NativeOutAmount)
				if crossedMarket[0].Factory.Hex() == HERMES_FACTORY_ADDRESS {
					allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex] = allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex].Add(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex], arb.TokenAmount).Sub(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex], new(big.Int).Div(arb.TokenAmount, big.NewInt(10000-uniswapV2FactoryAddressFeePerTenThousands[crossedMarket[0].TokenAddresses[crossedMarket[0].TokenIndex].Hex()])))
				} else {
					allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex] = allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex].Add(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex], arb.TokenAmount)

				}
				allMarketReserves[crossedMarket[0].TokenReserveIndex][2] = UPDATED_RESERVE
			} else {
				allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].NativeIndex] = allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].NativeIndex].Add(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].NativeIndex], arb.NativeOutAmount)
				if crossedMarket[0].Factory.Hex() == HERMES_FACTORY_ADDRESS {
					allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex] = allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex].Add(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex], new(big.Int).Div(arb.TokenAmount, big.NewInt(10000-uniswapV2FactoryAddressFeePerTenThousands[crossedMarket[0].TokenAddresses[crossedMarket[0].TokenIndex].Hex()]))).Sub(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex], arb.TokenAmount)

				} else {
					allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex] = allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex].Sub(allMarketReserves[crossedMarket[0].TokenReserveIndex][crossedMarket[0].TokenIndex], arb.TokenAmount)

				}
				allMarketReserves[crossedMarket[0].TokenReserveIndex][2] = UPDATED_RESERVE
			}

			// Update prices
			// How much token will I get from BASE_WEI Metis
			marketPairsByToken[crossedMarket[0].TokenAddresses[crossedMarket[0].TokenIndex]][count].SellWethPrice = ethmarket.GetAmountOut(allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
				allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
				BASE_WEI,
				pair.FeePerTenThousands)

			// How much token do I need to buy back BASE_WEI Metis
			marketPairsByToken[crossedMarket[0].TokenAddresses[crossedMarket[0].TokenIndex]][count].BuyWethPrice = ethmarket.GetAmountIn(allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
				allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
				BASE_WEI,
				pair.FeePerTenThousands)

			break
		}
	}
}

func priceMarkets() {
	for token, pairs := range marketPairsByToken {
		for count, pair := range pairs {
			// Figure out prices with Metis as reference
			// How much token will I get from 0.1 Metis
			marketPairsByToken[token][count].SellWethPrice = ethmarket.GetAmountOut(allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
				allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
				BASE_WEI,
				pair.FeePerTenThousands)

			// How much token do I need to buy back 0.1 Metis
			marketPairsByToken[token][count].BuyWethPrice = ethmarket.GetAmountIn(allMarketReserves[pair.TokenReserveIndex][pair.TokenIndex],
				allMarketReserves[pair.TokenReserveIndex][pair.NativeIndex],
				BASE_WEI,
				pair.FeePerTenThousands)
		}
	}
}

func isStaleReserves(buyFromPair models.UniswappyV2Pair, sellToPair models.UniswappyV2Pair) bool {
	return allMarketReserves[buyFromPair.TokenReserveIndex][2].Cmp(STALE_RESERVE) == 0 && allMarketReserves[sellToPair.TokenReserveIndex][2].Cmp(STALE_RESERVE) == 0
}

func markReservesAsStale() {
	// We use index 2 (which are reserve updated timestamps from our FlashQuery) to indicate stale and updated reserves
	// Later, we skip stale reserves because we should have previously analyzed them and found no opportunities
	for count := range allMarketReserves {
		allMarketReserves[count][2] = STALE_RESERVE
	}
}

func mapMarketAddresses() {
	// Create mapping
	for tokenAddress, pairs := range marketPairsByToken {
		for count, pair := range pairs {
			marketMapping[pair.MarketAdress] = models.MarketMapping{
				TokenAddress: tokenAddress,
				Index:        count,
			}
		}
	}
}

func sortMartkets() {
	// Sort each token market pair by their liquidity
	// If there is imbalance, the first arb we find would be between two most liquid pairs
	for token, pairs := range marketPairsByToken {
		sort.Slice(pairs, func(i, j int) bool {
			return allMarketReserves[pairs[i].TokenReserveIndex][pairs[i].NativeIndex].Cmp(allMarketReserves[pairs[j].TokenReserveIndex][pairs[j].NativeIndex]) > 0
		})

		marketPairsByToken[token] = pairs
	}
}
