// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

contract CityHealthCalculator is Ownable {
    using SafeMath for uint256;

    constructor() {
        _transferOwnership(msg.sender);
    }
    using SafeMath for uint256;
    using SafeMath for uint256;

    struct DailyEmission {
        uint256 timestamp;
        uint256 value;      // Scaled by 1e18
        bool recorded;
    }

    struct SectorHealth {
        mapping(uint256 => DailyEmission) dailyEmissions;
        uint256[] recordedDates;
        uint256 rollingAverage;    // 7-day average, scaled by 1e18
        uint256 baselineEmission;  // Reference point for health calculation
        uint256 worstEmission;     // Highest recorded emission
        bool isActive;
    }

    struct CityHealth {
        uint256 healthIndex;           // 0-100 scale, 100 being healthiest
        uint256 rollingHealthScore;    // 7-day average health score
        bool isVulnerable;
        mapping(string => SectorHealth) sectors;
        string[] activeSectors;
        bool exists;
    }

    // State variables
    mapping(string => CityHealth) public cities;
    string[] public registeredCities;

    // Thresholds (scaled by 1e18)
    uint256 public constant HEALTHY_THRESHOLD = 80 * 1e18;        // 80/100 score
    uint256 public constant VULNERABLE_THRESHOLD = 50 * 1e18;     // 50/100 score
    uint256 public constant MAX_SAFE_EMISSION = 0.001 * 1e18;     // Based on data pattern

    // Events
    event CityHealthUpdated(
        string indexed cityName,
        uint256 healthIndex,
        bool isVulnerable,
        uint256 timestamp
    );
    event SectorEmissionRecorded(
        string indexed cityName,
        string indexed sector,
        uint256 value,
        uint256 timestamp
    );
    event VulnerabilityAlert(
        string indexed cityName,
        uint256 healthIndex,
        uint256 timestamp
    );

    // Initialize a city
    function initializeCity(string memory cityName) external onlyOwner {
        require(!cities[cityName].exists, "City already initialized");
        
        cities[cityName].exists = true;
        cities[cityName].healthIndex = 100 * 1e18; // Start at perfect health
        registeredCities.push(cityName);
    }

    // Add sector to city
    function addSector(
        string memory cityName,
        string memory sectorName,
        uint256 baselineEmission
    ) external onlyOwner {
        require(cities[cityName].exists, "City not initialized");
        require(!cities[cityName].sectors[sectorName].isActive, "Sector already exists");

        SectorHealth storage newSector = cities[cityName].sectors[sectorName];
        newSector.isActive = true;
        newSector.baselineEmission = baselineEmission;
        cities[cityName].activeSectors.push(sectorName);
    }

    // Record daily emission and update health
    function recordEmissionAndUpdateHealth(
        string memory cityName,
        string memory sectorName,
        uint256 timestamp,
        uint256 emissionValue
    ) external onlyOwner {
        require(cities[cityName].exists, "City not initialized");
        require(cities[cityName].sectors[sectorName].isActive, "Sector not active");

        // Scale the emission value
        uint256 scaledEmission = emissionValue * 1e18;
        
        SectorHealth storage sector = cities[cityName].sectors[sectorName];
        
        // Record emission
        require(!sector.dailyEmissions[timestamp].recorded, "Emission already recorded for this date");
        sector.dailyEmissions[timestamp] = DailyEmission({
            timestamp: timestamp,
            value: scaledEmission,
            recorded: true
        });
        sector.recordedDates.push(timestamp);

        // Update worst emission if necessary
        if (scaledEmission > sector.worstEmission) {
            sector.worstEmission = scaledEmission;
        }

        // Update rolling average
        updateRollingAverage(cityName, sectorName);
        
        // Calculate new health index
        calculateCityHealth(cityName);

        emit SectorEmissionRecorded(cityName, sectorName, scaledEmission, timestamp);
    }

    // Update 7-day rolling average for a sector
    function updateRollingAverage(string memory cityName, string memory sectorName) internal {
        SectorHealth storage sector = cities[cityName].sectors[sectorName];
        
        uint256 daysToAverage = 7;
        uint256 totalEmissions = 0;
        uint256 count = 0;
        
        for (uint256 i = sector.recordedDates.length; i > 0 && count < daysToAverage; i--) {
            totalEmissions = totalEmissions.add(
                sector.dailyEmissions[sector.recordedDates[i-1]].value
            );
            count++;
        }
        
        if (count > 0) {
            sector.rollingAverage = totalEmissions.div(count);
        }
    }

    // Calculate city health based on sector emissions
    function calculateCityHealth(string memory cityName) internal {
        CityHealth storage city = cities[cityName];
        uint256 totalWeight = 0;
        uint256 weightedHealth = 0;

        for (uint256 i = 0; i < city.activeSectors.length; i++) {
            string memory sectorName = city.activeSectors[i];
            SectorHealth storage sector = city.sectors[sectorName];
            
            if (sector.rollingAverage > 0) {
                // Calculate sector health (100 = best, 0 = worst)
                uint256 sectorHealth;
                if (sector.rollingAverage <= MAX_SAFE_EMISSION) {
                    sectorHealth = 100 * 1e18;
                } else {
                    // Inverse relationship: higher emissions = lower health
                    sectorHealth = (MAX_SAFE_EMISSION * 100 * 1e18).div(sector.rollingAverage);
                }
                
                // For now, all sectors have equal weight
                totalWeight = totalWeight.add(1e18);
                weightedHealth = weightedHealth.add(sectorHealth);
            }
        }

        if (totalWeight > 0) {
            uint256 newHealthIndex = weightedHealth.div(totalWeight);
            city.healthIndex = newHealthIndex;
            
            // Update vulnerability status
            bool wasVulnerable = city.isVulnerable;
            city.isVulnerable = newHealthIndex < VULNERABLE_THRESHOLD;

            emit CityHealthUpdated(cityName, newHealthIndex, city.isVulnerable, block.timestamp);

            // Emit vulnerability alert if status changed to vulnerable
            if (!wasVulnerable && city.isVulnerable) {
                emit VulnerabilityAlert(cityName, newHealthIndex, block.timestamp);
            }
        }
    }

    // View functions
    function getCityHealth(string memory cityName) 
        external 
        view 
        returns (
            uint256 healthIndex,
            bool isVulnerable,
            uint256 numSectors
        ) 
    {
        require(cities[cityName].exists, "City not initialized");
        CityHealth storage city = cities[cityName];
        return (
            city.healthIndex,
            city.isVulnerable,
            city.activeSectors.length
        );
    }

    function getSectorHealth(string memory cityName, string memory sectorName)
        external
        view
        returns (
            uint256 rollingAverage,
            uint256 worstEmission,
            uint256 numReadings
        )
    {
        require(cities[cityName].exists, "City not initialized");
        require(cities[cityName].sectors[sectorName].isActive, "Sector not active");
        
        SectorHealth storage sector = cities[cityName].sectors[sectorName];
        return (
            sector.rollingAverage,
            sector.worstEmission,
            sector.recordedDates.length
        );
    }
}
