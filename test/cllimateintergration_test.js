// test/Integration.test.js
const CityRegister = artifacts.require("CityRegister");
const CityEmissions = artifacts.require("CityEmissionsContract");
const RenewalTheory = artifacts.require("RenewalTheoryContract");

contract("Integration Tests", accounts => {
  let cityRegister, cityEmissions, renewalTheory;
  const owner = accounts[0];
  const city = "Melbourne";
  const sector = "Aviation";

  beforeEach(async () => {
    cityRegister = await CityRegister.new({ from: owner });
    cityEmissions = await CityEmissions.new({ from: owner });
    renewalTheory = await RenewalTheory.new({ from: owner });
  });

  it("should process emissions data and calculate renewal metrics", async () => {
    // Register city
    await cityRegister.registerCity(city, { from: owner });

    // Add emissions data
    const timestamp = Math.floor(Date.now() / 1000);
    const emissionValue = web3.utils.toWei("0.000736959", "ether");
    
    await cityEmissions.addEmissionData(
      city,
      timestamp,
      sector,
      emissionValue,
      80, // AQI
      { from: owner }
    );

    // Process renewal theory calculations
    await renewalTheory.processRenewal(city, sector);

    // Verify results
    const renewalData = await renewalTheory.getSectorStats(city, sector);
    assert.notEqual(renewalData.totalRecordings.toNumber(), 0, "No recordings found");
  });
});