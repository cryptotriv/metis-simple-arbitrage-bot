package metis_simple_arbitrage

import (
	"math/big"
	"sync"

	"github.com/cryptotriv/raikiri/lib/models"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/fatih/color"
	"go.uber.org/zap"
)

var (
	_botColor color.Attribute
	_log_ch   chan<- models.MessageLog

	uniswapV2FactoryAddressFeePerTenThousands = map[string]int64{
		NETSWAP_FACTORY_ADDRESS:    30,
		AGORASWAP_FACTORY_ADDRESS:  10,
		TETHYS_FACTORY_ADDRESS:     20,
		HERMES_FACTORY_ADDRESS:     1,
		STANDARD_FACTORY_ADDRESS:   30,
		UNKNOWN_FACTORY_ADDRESS:    20,
		METIDORIAN_FACTORY_ADDRESS: 25,
		// MINIME_FACTORY_ADDRESS:    30,
	}

	uniswapV2FactoryAddresses = [...]string{
		NETSWAP_FACTORY_ADDRESS,
		AGORASWAP_FACTORY_ADDRESS,
		TETHYS_FACTORY_ADDRESS,
		HERMES_FACTORY_ADDRESS,
		STANDARD_FACTORY_ADDRESS,
		UNKNOWN_FACTORY_ADDRESS,
		METIDORIAN_FACTORY_ADDRESS,
		// MINIME_FACTORY_ADDRESS,
	}
	bannedTokenAddresses []string

	allMarketAddresses        []common.Address
	allMarketAddressFactories []common.Address
	allMarketReserves         [][3]*big.Int
	marketPairsByToken        map[common.Address][]models.UniswappyV2Pair = make(map[common.Address][]models.UniswappyV2Pair)
	marketMapping             map[common.Address]models.MarketMapping     = make(map[common.Address]models.MarketMapping)

	exit                 = false
	DEBUG                = false
	PRIVATE_KEY_EXECUTOR string

	allConfig models.AllBotsConfig
	config    models.SimpleArbitrageBot

	logger     *zap.Logger
	mainWg     *sync.WaitGroup
	botContext models.BotContext

	readClient  *ethclient.Client
	writeClient *ethclient.Client

	BASE_WEI                *big.Int
	MIN_NATIVE_AMOUNT_WEI   *big.Int
	MIN_PROFIT_WEI          *big.Int
	MIN_PROFIT_WEI_FOLLOWUP *big.Int
	STALE_RESERVE           *big.Int
	UPDATED_RESERVE         *big.Int
	MIN_GAS_GWEI            *big.Int

	uniswapV2ABI    abi.ABI
	hermesV1ABI     abi.ABI
	uniV2EventHash  common.Hash
	hermesEventHash common.Hash
	reservesUpdate  models.ReservesSyncEvent

	mu             sync.Mutex
	arbTxSentCount = 0
	nonce          uint64
)
