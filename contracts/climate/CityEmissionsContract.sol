// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract CityEmissionsContract {
    using SafeMath for uint256;

    struct EmissionData {
        uint256 timestamp;
        string sector;
        uint256 value;
        uint256 airQualityIndex;
        uint256 temperature;    // in Celsius * 100 for precision
    }

    struct CityMetrics {
        uint256 totalEmissions;
        uint256 averageTemperature;
        uint256 averageAQI;
        uint256 lastUpdateTime;
        uint256 carbonCreditsRequested;
        uint256 renewalCount;
        uint256 lastRenewalTime;
    }

    // Constants
    uint256 public constant RENEWAL_PERIOD = 30 days;
    uint256 public constant MAX_RENEWALS = 12;  // Maximum renewals per year
    uint256 public constant TEMP_REDUCTION_PER_CREDIT = 1;  // 0.01°C per credit
    uint256 public constant BASE_CREDIT_COST = 1 ether;
    uint256 public constant CARBON_TO_TEMP_FACTOR = 100;  // Conversion factor

    mapping(string => mapping(uint256 => EmissionData)) public cityEmissions;
    mapping(string => CityMetrics) public cityMetrics;
    mapping(string => uint256[]) public cityEmissionTimestamps;

    event EmissionDataAdded(string city, uint256 timestamp, string sector, uint256 value);
    event CarbonCreditRequested(string city, uint256 amount, uint256 cost);
    event RenewalProcessed(string city, uint256 timestamp, uint256 creditsUsed);

    // Add emission data for a city
    function addEmissionData(
        string memory city,
        uint256 timestamp,
        string memory sector,
        uint256 value,
        uint256 aqi
    ) external {
        // Calculate temperature based on emissions (simplified formula)
        uint256 temperature = calculateTemperature(value);
        
        cityEmissions[city][timestamp] = EmissionData({
            timestamp: timestamp,
            sector: sector,
            value: value,
            airQualityIndex: aqi,
            temperature: temperature
        });

        cityEmissionTimestamps[city].push(timestamp);
        updateCityMetrics(city, value, aqi, temperature);
        
        emit EmissionDataAdded(city, timestamp, sector, value);
    }

    // Calculate temperature from emissions using simplified formula
    function calculateTemperature(uint256 emissions) public pure returns (uint256) {
        // T = T_base + (emissions * CARBON_TO_TEMP_FACTOR)
        uint256 baseTemp = 1500; // 15.00°C
        return baseTemp.add(emissions.mul(CARBON_TO_TEMP_FACTOR));
    }

    // Update city metrics
    function updateCityMetrics(
        string memory city,
        uint256 newEmissions,
        uint256 newAQI,
        uint256 newTemp
    ) internal {
        CityMetrics storage metrics = cityMetrics[city];
        
        if (metrics.lastUpdateTime == 0) {
            metrics.totalEmissions = newEmissions;
            metrics.averageTemperature = newTemp;
            metrics.averageAQI = newAQI;
        } else {
            metrics.totalEmissions = metrics.totalEmissions.add(newEmissions);
            metrics.averageTemperature = (metrics.averageTemperature.add(newTemp)).div(2);
            metrics.averageAQI = (metrics.averageAQI.add(newAQI)).div(2);
        }
        
        metrics.lastUpdateTime = block.timestamp;
    }

    // Calculate required carbon credits based on renewal theory
    function calculateRequiredCredits(string memory city) public view returns (uint256) {
        CityMetrics storage metrics = cityMetrics[city];
        require(metrics.lastUpdateTime > 0, "No data for city");

        // Calculate based on temperature difference from target (20°C)
        uint256 targetTemp = 2000; // 20.00°C
        if (metrics.averageTemperature <= targetTemp) return 0;

        uint256 tempDiff = metrics.averageTemperature.sub(targetTemp);
        return tempDiff.div(TEMP_REDUCTION_PER_CREDIT);
    }

    // Request carbon credits using renewal theory
    function requestCarbonCredits(string memory city) external payable returns (uint256) {
        CityMetrics storage metrics = cityMetrics[city];
        require(block.timestamp.sub(metrics.lastRenewalTime) >= RENEWAL_PERIOD, "Renewal period not elapsed");
        require(metrics.renewalCount < MAX_RENEWALS, "Max renewals reached");

        uint256 requiredCredits = calculateRequiredCredits(city);
        uint256 cost = requiredCredits.mul(BASE_CREDIT_COST);
        require(msg.value >= cost, "Insufficient payment");

        metrics.carbonCreditsRequested = metrics.carbonCreditsRequested.add(requiredCredits);
        metrics.renewalCount = metrics.renewalCount.add(1);
        metrics.lastRenewalTime = block.timestamp;

        // Calculate temperature reduction
        uint256 tempReduction = requiredCredits.mul(TEMP_REDUCTION_PER_CREDIT);
        metrics.averageTemperature = metrics.averageTemperature.sub(tempReduction);

        emit CarbonCreditRequested(city, requiredCredits, cost);
        emit RenewalProcessed(city, block.timestamp, requiredCredits);

        return requiredCredits;
    }

    // Get city metrics
    function getCityMetrics(string memory city) external view returns (
        uint256 totalEmissions,
        uint256 avgTemp,
        uint256 avgAQI,
        uint256 creditsRequested,
        uint256 renewals
    ) {
        CityMetrics storage metrics = cityMetrics[city];
        return (
            metrics.totalEmissions,
            metrics.averageTemperature,
            metrics.averageAQI,
            metrics.carbonCreditsRequested,
            metrics.renewalCount
        );
    }
}
