// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/utils/math/SafeMath.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@uniswap/v2-periphery/contracts/interfaces/IUniswapV2Router02.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";

// Base contract for handling renewal theory calculations
contract RenewalTheoryContract {

    
    using SafeMath for uint256;

    struct SectorData {
        uint256 value;
        uint256 timestamp;
        uint256 totalRenewals;
        uint256[] renewalTimes;
        uint256[] creditAmounts;
        uint256 cumulativeReduction;
    }

    struct CityData {
        mapping(string => SectorData) sectors;
        uint256 baseAllowance;
        uint256 seasonalFactor;
        uint256 emergencyBuffer;
        uint256 lastCalculationTime;
    }

    mapping(string => CityData) public cities;

    // Constants
    uint256 public constant MIN_RENEWAL_INTERVAL = 1 days;
    uint256 public constant MAX_RENEWAL_RATE = 365; // Daily data
    uint256 public constant BASE_IMPACT = 100; // 1 unit in basis points
    uint256 public constant SEASONAL_VARIATION = 20; // 20% seasonal variation

    event RenewalProcessed(string city, string sector, uint256 timestamp, uint256 credits);
    event AllowanceUpdated(string city, uint256 newAllowance);
    event ReductionRecorded(string city, string sector, uint256 reduction);

    function initializeCity(string memory city, uint256 baseAllowance) external {
        require(cities[city].baseAllowance == 0, "City already initialized");
        
        cities[city].baseAllowance = baseAllowance;
        cities[city].seasonalFactor = 100;
        cities[city].emergencyBuffer = baseAllowance.div(10);
        cities[city].lastCalculationTime = block.timestamp;
    }

    function recordSectorValue(
        string memory city,
        string memory sector,
        uint256 value,
        uint256 timestamp
    ) external {
        require(cities[city].baseAllowance != 0, "City not initialized");
        
        SectorData storage sectorData = cities[city].sectors[sector];
        sectorData.value = value;
        sectorData.timestamp = timestamp;
        
        processRenewal(city, sector);
    }

    function processRenewal(string memory city, string memory sector) internal {
        SectorData storage sectorData = cities[city].sectors[sector];
        
        if (canRenew(city, sector)) {
            uint256 credits = calculateCreditAmount(city, sector);
            uint256 reduction = calculateReduction(credits);

            sectorData.totalRenewals = sectorData.totalRenewals.add(1);
            sectorData.renewalTimes.push(block.timestamp);
            sectorData.creditAmounts.push(credits);
            sectorData.cumulativeReduction = sectorData.cumulativeReduction.add(reduction);

            emit RenewalProcessed(city, sector, block.timestamp, credits);
            emit ReductionRecorded(city, sector, reduction);
        }
    }

    function canRenew(string memory city, string memory sector) public view returns (bool) {
        SectorData storage sectorData = cities[city].sectors[sector];
        
        if (sectorData.totalRenewals == 0) return true;
        
        uint256 lastRenewalTime = sectorData.renewalTimes[sectorData.renewalTimes.length - 1];
        uint256 timeSinceLastRenewal = block.timestamp.sub(lastRenewalTime);
        
        return timeSinceLastRenewal >= MIN_RENEWAL_INTERVAL;
    }

    function calculateCreditAmount(string memory city, string memory sector) public view returns (uint256) {
        CityData storage cityData = cities[city];
        SectorData storage sectorData = cityData.sectors[sector];
        
        uint256 baseAmount = cityData.baseAllowance.mul(cityData.seasonalFactor).div(100);
        
        if (isEmergencyCondition(city, sector)) {
            baseAmount = baseAmount.add(cityData.emergencyBuffer);
        }
        
        return baseAmount.mul(sectorData.value);
    }

    function calculateReduction(uint256 credits) public pure returns (uint256) {
        return credits.mul(BASE_IMPACT).div(1e4);
    }

    function isEmergencyCondition(string memory city, string memory sector) public view returns (bool) {
        SectorData storage sectorData = cities[city].sectors[sector];
        return sectorData.value > 0.001 ether; // Example threshold
    }
}

contract CarbonCreditMarket is RenewalTheoryContract, Ownable, ReentrancyGuard, Pausable {
    using SafeMath for uint256;

    IUniswapV2Router02 public immutable uniswapRouter;
    address public immutable carbonCreditToken;
    address public immutable usdToken;
    
    uint256 public constant MIN_TRADE_AMOUNT = 1e18;
    uint256 public constant MAX_TRADE_AMOUNT = 1000000e18;
    uint256 public constant SLIPPAGE_TOLERANCE = 50; // 0.5%

    struct CompanyData {
        bool isRegistered;
        uint256 carbonCredits;
        bool isBuying;
        bool isSelling;
        uint256 lastTradeTimestamp;
    }

    mapping(address => CompanyData) public companies;
    address[] private buyersList;
    address[] private sellersList;

    event CompanyRegistered(address indexed company);
    event BuyerAdded(address indexed buyer);
    event SellerAdded(address indexed seller);
    event TradeExecuted(
        address indexed buyer,
        address indexed seller,
        uint256 carbonCredits,
        uint256 usdAmount,
        string city,
        string sector
    );
constructor(
    address _uniswapRouter,
    address _carbonCreditToken,
    address _usdToken
) {
    require(_uniswapRouter != address(0), "Invalid router address");
    require(_carbonCreditToken != address(0), "Invalid carbon token address");
    require(_usdToken != address(0), "Invalid USD token address");
    
    uniswapRouter = IUniswapV2Router02(_uniswapRouter);
    carbonCreditToken = _carbonCreditToken;
    usdToken = _usdToken;
    
    _transferOwnership(msg.sender);
}

    modifier onlyRegisteredCompany() {
        require(companies[msg.sender].isRegistered, "Company not registered");
        _;
    }

    modifier validTradeAmount(uint256 amount) {
        require(amount >= MIN_TRADE_AMOUNT, "Trade amount too small");
        require(amount <= MAX_TRADE_AMOUNT, "Trade amount too large");
        _;
    }

    function registerCompany(address company) external onlyOwner {
        require(company != address(0), "Invalid company address");
        require(!companies[company].isRegistered, "Company already registered");
        
        companies[company].isRegistered = true;
        companies[company].lastTradeTimestamp = 0;
        
        emit CompanyRegistered(company);
    }

    function wantToBuy() external onlyRegisteredCompany whenNotPaused {
        require(!companies[msg.sender].isBuying, "Already in buyers list");
        require(!companies[msg.sender].isSelling, "Cannot buy while selling");
        
        companies[msg.sender].isBuying = true;
        buyersList.push(msg.sender);
        
        emit BuyerAdded(msg.sender);
    }

    function wantToSell() external onlyRegisteredCompany whenNotPaused {
        require(!companies[msg.sender].isSelling, "Already in sellers list");
        require(!companies[msg.sender].isBuying, "Cannot sell while buying");
        
        // Check if seller has enough carbon credits
        uint256 balance = IERC20(carbonCreditToken).balanceOf(msg.sender);
        require(balance >= MIN_TRADE_AMOUNT, "Insufficient carbon credits");
        
        companies[msg.sender].isSelling = true;
        sellersList.push(msg.sender);
        
        emit SellerAdded(msg.sender);
    }

    function trade(
        address buyer,
        address seller,
        uint256 carbonCredits,
        uint256 usdAmount,
        string memory city,
        string memory sector
    ) 
        external 
        onlyOwner 
        whenNotPaused 
        nonReentrant 
        validTradeAmount(carbonCredits) 
    {
        require(companies[buyer].isBuying, "Buyer not interested");
        require(companies[seller].isSelling, "Seller not interested");
        require(
            IERC20(carbonCreditToken).balanceOf(seller) >= carbonCredits,
            "Insufficient seller balance"
        );
        require(
            IERC20(usdToken).balanceOf(buyer) >= usdAmount,
            "Insufficient buyer USD balance"
        );
        
        SectorData storage sectorData = cities[city].sectors[sector];
        require(
            carbonCredits.mul(4) > sectorData.cumulativeReduction.mul(3),
            "Insufficient carbon credits"
        );

        // Check for allowances
        require(
            IERC20(carbonCreditToken).allowance(seller, address(this)) >= carbonCredits,
            "Insufficient carbon credit allowance"
        );
        require(
            IERC20(usdToken).allowance(buyer, address(this)) >= usdAmount,
            "Insufficient USD allowance"
        );

        // Get expected swap amount
        uint256 expectedAmount = getExpectedSwapAmount(carbonCredits);
        require(
            usdAmount >= expectedAmount.mul(1000 - SLIPPAGE_TOLERANCE).div(1000),
            "Price slippage too high"
        );

        // Execute trades
        bool success = executeTokenTransfers(buyer, seller, carbonCredits, usdAmount);
        require(success, "Trade execution failed");

        // Update company status
        updateTradeStatus(buyer, seller);
        
        emit TradeExecuted(buyer, seller, carbonCredits, usdAmount, city, sector);
    }

    function executeTokenTransfers(
        address buyer,
        address seller,
        uint256 carbonCredits,
        uint256 usdAmount
    ) private returns (bool) {
        // Transfer carbon credits
        require(
            IERC20(carbonCreditToken).transferFrom(seller, buyer, carbonCredits),
            "Carbon credit transfer failed"
        );
        
        // Transfer USD
        require(
            IERC20(usdToken).transferFrom(buyer, seller, usdAmount),
            "USD transfer failed"
        );
        
        return true;
    }

    function updateTradeStatus(address buyer, address seller) private {
        removeFromList(buyer, buyersList);
        removeFromList(seller, sellersList);
        
        companies[buyer].isBuying = false;
        companies[seller].isSelling = false;
        companies[buyer].lastTradeTimestamp = block.timestamp;
        companies[seller].lastTradeTimestamp = block.timestamp;
    }

    function removeFromList(address company, address[] storage list) private {
        for (uint256 i = 0; i < list.length; i++) {
            if (list[i] == company) {
                list[i] = list[list.length - 1];
                list.pop();
                break;
            }
        }
    }

    function getExpectedSwapAmount(uint256 carbonCredits) public view returns (uint256) {
        address[] memory path = new address[](2);
        path[0] = carbonCreditToken;
        path[1] = usdToken;
        
        uint256[] memory amounts = uniswapRouter.getAmountsOut(carbonCredits, path);
        return amounts[1];
    }

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

    // Emergency withdrawal function
    function emergencyWithdraw(address token) external onlyOwner {
        uint256 balance = IERC20(token).balanceOf(address(this));
        require(balance > 0, "No balance to withdraw");
        IERC20(token).transfer(owner(), balance);
    }
}
