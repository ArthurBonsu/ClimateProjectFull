// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "./CityRegister.sol";

contract CompanyRegister is CityRegister {
constructor(string memory name, string memory symbol) CityRegister(name, symbol) {
    // Additional initialization if needed
    _transferOwnership(msg.sender);
}

    string private _tokenName = "RPSTOKENS";
    string private _tokenSymbol = "RPS";

    struct Company {
        address payable companyAddress;
        address city;
        string location;
        uint256 longitude;
        uint256 latitude;
        uint256 carbonCapacity;
       
    }

    mapping(address => bool) public registeredCompanies;
    mapping(address => bool) public paidCompanyEscrowFee;
    mapping(address => mapping(address => bool)) public checkIfCompanyIsInCity;

    mapping(address => Company) public companyStore;
    mapping(address => mapping(uint256 => uint256)) public temperature;
    mapping(address => mapping(uint256 => uint256)) public carbonCapacityCompany;
    mapping(address => mapping(uint256 => uint256)) public humidity;

    mapping(address => uint256) public companycarbonLevels;
    mapping(address => uint256) public companymaxCreditLevels;
    mapping(address => uint256) public companycarboncredit;



    function payFees(address payable sender, uint256 amount) public payable {
        require(amount >= 10 ether, "Amount must be at least 10 ether");
        (bool success, ) = sender.call{value: amount}("");
        require(success, "Payment failed");
        paidCompanyEscrowFee[sender] = true;
    }

    function registerCompany(  
        address payable companyAddress,
        address cityAddress,
        uint256 amount,
        uint256 lng,
        uint256 lat,
        uint256 carbonCapacity
    ) external onlyOwner returns (address,address,uint256, string memory , uint256) {
        require(paidCompanyEscrowFee[companyAddress] == false, "Company fee already paid");
        require(registeredCompanies[companyAddress] == false, "Company already registered");

        payFees(companyAddress, amount);

        registeredCompanies[companyAddress] = true;

        string memory myLocation = getLocation(lng, lat);

        companyStore[companyAddress] = Company({
            companyAddress: companyAddress,
            city: cityAddress,
            location: myLocation,
            longitude: lng,
            latitude: lat,
            carbonCapacity: carbonCapacity
       
        });

        checkIfCompanyIsInCity[cityAddress][companyAddress] = true;

         return (
            companyAddress, 
            cityAddress ,  
            amount,
            myLocation, 
            carbonCapacity
            
     // You need to determine the companyCountId
        );
    }

    function getLatitude(uint256 lat) external pure returns (uint256) {
        return lat;
    }

    function getLongitude(uint256 lng) external pure returns (uint256) {
        return lng;
    }

    function getLocation(uint256 lat, uint256 lng) internal pure returns (string memory) {
        return string(abi.encodePacked("Lat: ", uintToStr(lat), ", Lng: ", uintToStr(lng)));
    }

    function uintToStr(uint256 value) internal pure returns (string memory) {
        if (value == 0) {
            return "0";
        }
        uint256 temp = value;
        uint256 digits;
        while (temp != 0) {
            digits++;
            temp /= 10;
        }
        bytes memory buffer = new bytes(digits);
        while (value != 0) {
            digits -= 1;
            buffer[digits] = bytes1(uint8(48 + uint256(value % 10)));
            value /= 10;
        }
        return string(buffer);
    }

    function setMaximumCompanyCarbonLevel(address cityAddress, uint256 maximumCarbonLevel) external returns (address, uint256) {
        // Check if the carbon level for the city is not set (initialized to 0)
        if (companycarbonLevels[cityAddress] == 0) {
            companymaxCreditLevels[cityAddress] = maximumCarbonLevel;
        }

        return (cityAddress, maximumCarbonLevel);
    }

    function getCompanyCarbonLevel(address cityAddress) external view returns (uint256) {
        return companycarbonLevels[cityAddress];
    }

    function getCompanyCarbonCredit(address cityAddress) external view returns (uint256) {
        uint256 companycarboncredits = companymaxCreditLevels[cityAddress] - companycarbonLevels[cityAddress];
        return companycarboncredits;
    }
}
