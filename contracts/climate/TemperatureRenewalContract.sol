// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

// Interface for existing credit market contract
interface ICreditMarketContract {
    function requestCarbonCredits(uint256 amount) external;
    function getPrice() external view returns (uint256);
}

contract TemperatureRenewalContract {
    struct CityTemperature {
        uint256 currentTemp;     // Current temperature in Kelvin * 1e18
        uint256 targetTemp;      // Target temperature in Kelvin * 1e18
        uint256 tickSize;        // Temperature reduction per tick * 1e18
        uint256 lastUpdateTime;  // Last update timestamp
        mapping(uint256 => uint256) historicalTemps; // timestamp -> temperature
    }

    struct EmissionData {
        string sector;
        uint256 value;
        uint256 timestamp;
    }

    mapping(string => CityTemperature) public cityTemperatures;
    mapping(string => EmissionData[]) public cityEmissions;
    
    uint256 public constant SECONDS_PER_TICK = 1 days;
    uint256 public constant PRECISION = 1e18;
    
    event TemperatureUpdated(string city, uint256 temperature, uint256 timestamp);
    event EmissionRecorded(string city, string sector, uint256 value, uint256 timestamp);
    event RenewalExecuted(string city, uint256 tempReduction, uint256 timestamp);

    // Calculate number of ticks needed for temperature reduction
    function calculateRequiredTicks(string memory city) public view returns (uint256) {
        CityTemperature storage cityTemp = cityTemperatures[city];
        require(cityTemp.currentTemp > 0, "City not initialized");
        
        if (cityTemp.currentTemp <= cityTemp.targetTemp) return 0;
        
        return ((cityTemp.currentTemp - cityTemp.targetTemp) * PRECISION) / cityTemp.tickSize;
    }

    // Calculate carbon intensity for a city over a time period
    function calculateCarbonIntensity(
        string memory city,
        uint256 startTime,
        uint256 endTime
    ) public view returns (uint256) {
        EmissionData[] storage emissions = cityEmissions[city];
        uint256 totalEmissions = 0;
        uint256 dataPoints = 0;
        
        for (uint256 i = 0; i < emissions.length; i++) {
            if (emissions[i].timestamp >= startTime && emissions[i].timestamp <= endTime) {
                totalEmissions += emissions[i].value;
                dataPoints++;
            }
        }
        
        return dataPoints > 0 ? totalEmissions / dataPoints : 0;
    }

    // Record new emission data
    function recordEmission(
        string memory city,
        string memory sector,
        uint256 value,
        uint256 timestamp
    ) external {
        EmissionData memory newData = EmissionData(sector, value, timestamp);
        cityEmissions[city].push(newData);
        emit EmissionRecorded(city, sector, value, timestamp);
    }

    // Execute renewal theory-based temperature reduction
    function executeRenewal(string memory city) external returns (uint256) {
        CityTemperature storage cityTemp = cityTemperatures[city];
        require(cityTemp.currentTemp > 0, "City not initialized");
        
        uint256 timePassed = block.timestamp - cityTemp.lastUpdateTime;
        uint256 ticks = timePassed / SECONDS_PER_TICK;
        
        if (ticks > 0 && cityTemp.currentTemp > cityTemp.targetTemp) {
            uint256 tempReduction = ticks * cityTemp.tickSize;
            
            // Ensure we don't reduce below target
            if (cityTemp.currentTemp - tempReduction < cityTemp.targetTemp) {
                tempReduction = cityTemp.currentTemp - cityTemp.targetTemp;
            }
            
            cityTemp.currentTemp -= tempReduction;
            cityTemp.lastUpdateTime = block.timestamp;
            
            emit RenewalExecuted(city, tempReduction, block.timestamp);
            return tempReduction;
        }
        
        return 0;
    }
}

contract ClimateReductionContract {
    struct ReductionTarget {
        uint256 initialEmissions;
        uint256 targetEmissions;
        uint256 deadline;
        uint256 currentEmissions;
        bool achieved;
    }

    mapping(string => ReductionTarget) public cityTargets;
    ICreditMarketContract public creditMarket;
    
    event TargetAchieved(string city, uint256 timestamp);
    event ReductionUpdated(string city, uint256 newEmissions, uint256 timestamp);

    constructor(address _creditMarket) {
        creditMarket = ICreditMarketContract(_creditMarket);
    }

    // Set reduction target for a city
    function setReductionTarget(
        string memory city,
        uint256 initial,
        uint256 target,
        uint256 timeframe
    ) external {
        cityTargets[city] = ReductionTarget({
            initialEmissions: initial,
            targetEmissions: target,
            deadline: block.timestamp + timeframe,
            currentEmissions: initial,
            achieved: false
        });
    }

    // Update current emissions and check target achievement
    function updateEmissions(string memory city, uint256 newEmissions) external {
        ReductionTarget storage target = cityTargets[city];
        require(target.initialEmissions > 0, "Target not set");
        
        target.currentEmissions = newEmissions;
        emit ReductionUpdated(city, newEmissions, block.timestamp);
        
        if (newEmissions <= target.targetEmissions && !target.achieved) {
            target.achieved = true;
            emit TargetAchieved(city, block.timestamp);
        }
    }

    // Calculate required carbon credits based on current vs target emissions
    function calculateRequiredCredits(string memory city) public view returns (uint256) {
        ReductionTarget storage target = cityTargets[city];
        if (target.currentEmissions <= target.targetEmissions) return 0;
        
        return (target.currentEmissions - target.targetEmissions) * creditMarket.getPrice();
    }
}
