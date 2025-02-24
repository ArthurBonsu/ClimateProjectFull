// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract CityRegister is ERC20, Ownable {
    using SafeMath for uint256;

    constructor(string memory name, string memory symbol) ERC20(name, symbol) {
        _mint(msg.sender, 1000000 * 10 ** decimals());
        _transferOwnership(msg.sender);
    }

    using SafeMath for uint256;

    // Struct to store daily measurement
    struct DailyMeasurement {
        uint256 timestamp;
        uint256 value;     // Stored as value * 1e18 for precision
        bool recorded;
    }

    // Struct to store sector data
    struct SectorData {
        mapping(uint256 => DailyMeasurement) dailyData;  // timestamp => measurement
        uint256[] recordedDates;
        uint256 baselineValue;
        uint256 rollingAverage;    // Stored as value * 1e18
        uint256 maxHistoricalValue;
        bool isActive;
    }

    struct City {
        string name;
        bool isRegistered;
        mapping(string => SectorData) sectors;
        string[] activeSectors;
        uint256 registrationDate;
    }

    mapping(string => City) public cities;
    string[] public registeredCityNames;
    
    uint256 public constant SCALE_FACTOR = 1e18;
    uint256 public constant ROLLING_AVERAGE_DAYS = 7;

    // Events
    event CityRegistered(string indexed cityName, uint256 timestamp);
    event SectorAdded(string indexed cityName, string indexed sectorName, uint256 timestamp);
    event DailyDataRecorded(
        string indexed cityName,
        string indexed sector,
        uint256 timestamp,
        uint256 value
    );

   

    modifier validCity(string memory cityName) {
        require(bytes(cityName).length > 0, "City name cannot be empty");
        require(cities[cityName].isRegistered, "City not registered");
        _;
    }

    modifier validSector(string memory cityName, string memory sectorName) {
        require(cities[cityName].sectors[sectorName].isActive, "Sector not active");
        _;
    }

  function registerCity(
    string memory cityName,
    string memory date,
    string memory sector,
    uint256 value
) external onlyOwner {
    require(bytes(cityName).length > 0, "City name cannot be empty");
    require(!cities[cityName].isRegistered, "City already registered");

    City storage newCity = cities[cityName];
    newCity.name = cityName;
    newCity.isRegistered = true;
    newCity.registrationDate = block.timestamp;

    registeredCityNames.push(cityName);
    
    emit CityRegistered(cityName, block.timestamp);
}
    function addSector(string memory cityName, string memory sectorName) 
        external 
        onlyOwner 
        validCity(cityName) 
    {
        require(!cities[cityName].sectors[sectorName].isActive, "Sector already exists");

        City storage city = cities[cityName];
        SectorData storage newSector = city.sectors[sectorName];
        newSector.isActive = true;
        city.activeSectors.push(sectorName);

        emit SectorAdded(cityName, sectorName, block.timestamp);
    }

    function recordDailyValue(
        string memory cityName,
        string memory sectorName,
        uint256 timestamp,
        uint256 value
    ) 
        external 
        onlyOwner 
        validCity(cityName)
        validSector(cityName, sectorName)
    {
        require(timestamp <= block.timestamp, "Future timestamp not allowed");
        require(value > 0, "Value must be greater than 0");

        uint256 scaledValue = value * SCALE_FACTOR;
        
        SectorData storage sector = cities[cityName].sectors[sectorName];
        
        DailyMeasurement storage measurement = sector.dailyData[timestamp];
        require(!measurement.recorded, "Data already recorded for this date");
        
        measurement.timestamp = timestamp;
        measurement.value = scaledValue;
        measurement.recorded = true;
        sector.recordedDates.push(timestamp);

        if (scaledValue > sector.maxHistoricalValue) {
            sector.maxHistoricalValue = scaledValue;
        }

        _updateRollingAverage(sector);

        emit DailyDataRecorded(cityName, sectorName, timestamp, scaledValue);
    }

    function _updateRollingAverage(SectorData storage sector) private {
        uint256 totalValues = 0;
        uint256 count = 0;
        
        uint256 length = sector.recordedDates.length;
        uint256 startIndex = length >= ROLLING_AVERAGE_DAYS ? length - ROLLING_AVERAGE_DAYS : 0;
        
        for (uint256 i = startIndex; i < length; i++) {
            totalValues = totalValues.add(sector.dailyData[sector.recordedDates[i]].value);
            count++;
        }
        
        if (count > 0) {
            sector.rollingAverage = totalValues.div(count);
        }
    }

    function getSectorStats(
        string memory cityName,
        string memory sectorName
    ) external view returns (
        uint256 totalRecordings,
        uint256 maxValue,
        uint256 rollingAverage
    ) {
        require(cities[cityName].isRegistered, "City not registered");
        require(cities[cityName].sectors[sectorName].isActive, "Sector not active");

        SectorData storage sector = cities[cityName].sectors[sectorName];
        return (
            sector.recordedDates.length,
            sector.maxHistoricalValue,
            sector.rollingAverage
        );
    }

    function getActiveSectors(string memory cityName) 
        external 
        view 
        returns (string[] memory) 
    {
        require(cities[cityName].isRegistered, "City not registered");
        return cities[cityName].activeSectors;
    }

    function calculateCarbonCredit(
        string memory cityName,
        string memory sectorName,
        uint256 timestamp
    ) external view returns (uint256) {
        require(cities[cityName].isRegistered, "City not registered");
        require(cities[cityName].sectors[sectorName].isActive, "Sector not active");

        SectorData storage sector = cities[cityName].sectors[sectorName];
        DailyMeasurement storage measurement = sector.dailyData[timestamp];
        require(measurement.recorded, "No data recorded for this date");

        if (measurement.value >= sector.maxHistoricalValue) {
            return 0;
        }

        return sector.maxHistoricalValue.sub(measurement.value);
    }
}
