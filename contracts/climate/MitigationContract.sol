// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@chainlink/contracts/src/v0.8/interfaces/AggregatorV3Interface.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";

contract MitigationContract is ReentrancyGuard {
    address public owner;
    AggregatorV3Interface internal carbonLevelFeed;
    AggregatorV3Interface internal temperatureFeed;

    struct CityData {
        uint256 currentTemperature;
        uint256 targetTemperature;
        uint256 carbonLevel;
        uint256 targetCarbonLevel;
        uint256 lastUpdateTime;
        uint256 deltaFactor;      // δ from your paper
        uint256 mitigationLevel;  // Threshold for climate vulnerable cities
        bool isVulnerable;
        mapping(string => uint256) sectorEmissions;
    }

    struct RenewalParameters {
        uint256 tickSize;         // ΔT_tick from your paper
        uint256 rewardRate;       // r from your paper
        uint256 salvageValue;     // v from your paper
        uint256 penaltyRate;      // p from your paper
        uint256 discountFactor;   // γ from your paper
    }

    mapping(string => CityData) public cities;
    mapping(address => uint256) public participantCarbonCredits;
    mapping(address => uint256) public participantRewards;
    RenewalParameters public renewalParams;

    // Constants
    uint256 public constant PRECISION = 1e18;
    uint256 public constant BASE_CREDIT_AMOUNT = 100 * PRECISION;
    
    // Decimal constants (multiplied by PRECISION)
    uint256 public constant TICK_SIZE = (PRECISION * 1) / 10;        // 0.1
    uint256 public constant REWARD_RATE = (PRECISION * 5) / 100;     // 0.05
    uint256 public constant SALVAGE_VALUE = (PRECISION * 80) / 100;  // 0.8
    uint256 public constant PENALTY_RATE = (PRECISION * 20) / 100;   // 0.2
    uint256 public constant DISCOUNT_FACTOR = (PRECISION * 95) / 100; // 0.95
    
    // Events remain the same
    event CarbonLevelMeasured(string city, uint256 carbonLevel);
    event TemperatureMeasured(string city, uint256 temperature);
    event CarbonCreditsAdjusted(address participant, uint256 amount);
    event RewardCalculated(address participant, uint256 reward);
    event CityStatusUpdated(string city, bool isVulnerable);
    event MitigationExecuted(string city, uint256 tempReduction, uint256 carbonReduction);

    constructor(
        address _carbonLevelFeed, 
        address _temperatureFeed
    ) {
        owner = msg.sender;
        carbonLevelFeed = AggregatorV3Interface(_carbonLevelFeed);
        temperatureFeed = AggregatorV3Interface(_temperatureFeed);
        
        // Initialize renewal parameters using the constant values
        renewalParams = RenewalParameters({
            tickSize: TICK_SIZE,
            rewardRate: REWARD_RATE,
            salvageValue: SALVAGE_VALUE,
            penaltyRate: PENALTY_RATE,
            discountFactor: DISCOUNT_FACTOR
        });
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "Only owner can call this function");
        _;
    }

    // Calculate required ticks for temperature reduction
    function calculateRequiredTicks(string memory cityName) public view returns (uint256) {
        CityData storage city = cities[cityName];
        if (city.currentTemperature <= city.targetTemperature) return 0;
        
        // Multiply by PRECISION before division to maintain precision
        return (city.currentTemperature - city.targetTemperature) * PRECISION / renewalParams.tickSize;
    }

    // Calculate total cost using updated decimal handling
    function calculateTotalCost(string memory cityName) public view returns (uint256) {
        CityData storage city = cities[cityName];
        uint256 annualEmissionReduction = (city.carbonLevel - city.targetCarbonLevel);
        uint256 temperatureDelta = (city.currentTemperature - city.targetTemperature);
        
        // Handle precision properly in multiplication
        return (annualEmissionReduction * temperatureDelta * getCurrentPrice()) / (PRECISION * PRECISION);
    }

    // Calculate temperature reduction with proper decimal handling
    function calculateTemperatureReduction(string memory cityName, uint256 ticks) internal view returns (uint256) {
        CityData storage city = cities[cityName];
        uint256 maxReduction = (city.currentTemperature - city.targetTemperature);
        // Multiply ticks by tickSize and divide by PRECISION to maintain precision
        uint256 theoreticalReduction = (ticks * renewalParams.tickSize) / PRECISION;
        
        return theoreticalReduction < maxReduction ? theoreticalReduction : maxReduction;
    }

    // Calculate carbon reduction with proper decimal handling
    function calculateCarbonReduction(string memory cityName, uint256 ticks) internal view returns (uint256) {
        CityData storage city = cities[cityName];
        // Handle precision in multiplication and division
        uint256 baseReduction = (city.carbonLevel * ticks * renewalParams.rewardRate) / PRECISION;
        
        if (city.isVulnerable) {
            // Add penalty rate while maintaining precision
            baseReduction = (baseReduction * (PRECISION + renewalParams.penaltyRate)) / PRECISION;
        }
        
        return baseReduction;
    }
    // Evaluate if a city is climate vulnerable based on your research criteria
    function evaluateCityVulnerability(string memory cityName) internal view returns (bool) {
        CityData storage city = cities[cityName];
        
        // City is vulnerable if it exceeds either threshold
        bool temperatureExceeded = city.currentTemperature > city.targetTemperature;
        bool carbonExceeded = city.carbonLevel > city.targetCarbonLevel;
        
        return temperatureExceeded || carbonExceeded;
    }

    // Calculate rewards using your renewal reward theorem
    function calculateReward(address participant, uint256 interactions) public {
        uint256 carbonCredits = participantCarbonCredits[participant];
        
        // R(t) = (m(t) + 1)E[R] - E[R_N(t)+1]
        uint256 baseReward = (interactions + 1) * carbonCredits;
        uint256 discountedReward = (baseReward * renewalParams.discountFactor) / PRECISION;
        
        participantRewards[participant] += discountedReward;
        emit RewardCalculated(participant, discountedReward);
    }

    // Helper function to get current carbon credit price
    function getCurrentPrice() internal view returns (uint256) {
        (, int256 price,,,) = carbonLevelFeed.latestRoundData();
        return uint256(price);
    }

    // Getter functions
    function getCityData(string memory cityName) external view returns (
        uint256 currentTemp,
        uint256 targetTemp,
        uint256 carbonLevel,
        uint256 targetCarbon,
        bool isVulnerable
    ) {
        CityData storage city = cities[cityName];
        return (
            city.currentTemperature,
            city.targetTemperature,
            city.carbonLevel,
            city.targetCarbonLevel,
            city.isVulnerable
        );
    }
}
