const UncertaintyAnalytics = artifacts.require("UncertaintyAnalytics");
const RequestManager = artifacts.require("RequestManager");
const ResponseManager = artifacts.require("ResponseManager");

contract("UncertaintyAnalytics", accounts => {
  let analytics;
  let requestManager;
  let responseManager;
  let startTime;
  const [owner, requester, responder] = accounts;

  const measureTime = async (fn) => {
    const start = Date.now();
    const result = await fn();
    const end = Date.now();
    return { result, duration: end - start };
  };

  const getGasCost = async (tx) => {
    const receipt = await web3.eth.getTransactionReceipt(tx.tx);
    return web3.utils.toBN(receipt.gasUsed).mul(web3.utils.toBN(tx.receipt.effectiveGasPrice));
  };

  const getDeploymentCost = async (contract) => {
    const receipt = await web3.eth.getTransactionReceipt(contract.transactionHash);
    return web3.utils.fromWei(
      web3.utils.toBN(receipt.gasUsed).mul(web3.utils.toBN(receipt.effectiveGasPrice))
    );
  };

  const advanceTime = async (seconds) => {
    await web3.currentProvider.send({
      jsonrpc: "2.0",
      method: "evm_increaseTime",
      params: [seconds],
      id: Date.now(),
    });
    await web3.currentProvider.send({
      jsonrpc: "2.0",
      method: "evm_mine",
      params: [],
      id: Date.now()
    });
  };

  beforeEach(async () => {
    analytics = await UncertaintyAnalytics.new();
    console.log("Analytics Deployment Cost:", await getDeploymentCost(analytics), "ETH");
    
    requestManager = await RequestManager.new(analytics.address);
    console.log("RequestManager Deployment Cost:", await getDeploymentCost(requestManager), "ETH");
    
    responseManager = await ResponseManager.new(analytics.address);
    console.log("ResponseManager Deployment Cost:", await getDeploymentCost(responseManager), "ETH");
    
    startTime = Date.now();
  });

  describe("Request-Response Flow Analysis", () => {
    it("Should analyze full request-response cycle with metrics", async () => {
      const metrics = {
        confirmationTimes: [],
        executionTimes: [],
        gasCosts: [],
        failedTransactions: 0,
        invalidAddressAttempts: 0,
        invalidRequirementAttempts: 0
      };

      console.log("\n=== Starting 10 Request-Response Cycles ===");

      // Submit 10 requests
      for(let i = 0; i < 10; i++) {
        const { duration: confirmTime, result: tx } = await measureTime(async () => {
          return await requestManager.submitRequest({
            from: requester,
            value: web3.utils.toWei("0.001", "ether")
          });
        });
        
        metrics.confirmationTimes.push(confirmTime);
        metrics.gasCosts.push(await getGasCost(tx));

        const { duration: execTime } = await measureTime(async () => {
          return await responseManager.submitResponse(i + 1, { from: responder });
        });
        
        metrics.executionTimes.push(execTime);
        
        if (i % 3 === 0) {
          await analytics.updateDisruptionLevel(i + 1, { from: owner });
          await analytics.updateEscalationLevel(Math.floor(i / 2), { from: owner });
        }
      }

      // Test invalid scenarios
      console.log("\n=== Testing Invalid Scenarios ===");
      
      try {
        await requestManager.submitRequest({ 
          from: "0x0000000000000000000000000000000000000000",
          value: web3.utils.toWei("0.001", "ether")
        });
      } catch {
        metrics.invalidAddressAttempts++;
      }

      try {
        await requestManager.submitRequest({
          from: requester,
          value: web3.utils.toWei("0.0001", "ether")
        });
      } catch {
        metrics.invalidRequirementAttempts++;
      }

      await advanceTime(86401); // Advance time by 1 day + 1 second
      await analytics.calculateUnavailabilityCost(1);
      
      const analyticsMetrics = await analytics.getMetrics();
      const unavailabilityCost = await analytics.unavailabilityCost();
      const disruptionLevel = await analytics.disruptionLevel();
      const escalationLevel = await analytics.escalationLevel();

      console.log("\n=== Performance Metrics ===");
      console.log("Average Confirmation Time:", 
        metrics.confirmationTimes.reduce((a, b) => a + b, 0) / metrics.confirmationTimes.length, "ms");
      console.log("Average Execution Time:", 
        metrics.executionTimes.reduce((a, b) => a + b, 0) / metrics.executionTimes.length, "ms");
      console.log("Average Gas Cost:", 
        web3.utils.fromWei(
          metrics.gasCosts.reduce((a, b) => a.add(b), web3.utils.toBN(0))
          .div(web3.utils.toBN(metrics.gasCosts.length))
        ), "ETH");

      console.log("\n=== Uncertainty Metrics ===");
      console.log("Success Rate:", analyticsMetrics.successRate.toString(), "%");
      console.log("Average Processing Time:", analyticsMetrics.avgProcessingTime.toString(), "seconds");
      console.log("Total Transaction Cost:", web3.utils.fromWei(analyticsMetrics.totalCost), "ETH");
      console.log("Unavailability Cost:", web3.utils.fromWei(unavailabilityCost), "ETH");
      console.log("Disruption Level:", disruptionLevel.toString());
      console.log("Escalation Level:", escalationLevel.toString());

      console.log("\n=== Error Metrics ===");
      console.log("Failed Transactions:", metrics.failedTransactions);
      console.log("Invalid Address Attempts:", metrics.invalidAddressAttempts);
      console.log("Invalid Requirement Attempts:", metrics.invalidRequirementAttempts);
      console.log("Total Disruptions:", analyticsMetrics.disruptionCount.toString());

      assert.isTrue(analyticsMetrics.successRate.gt(web3.utils.toBN(0)), "Success rate should be positive");
      assert.isTrue(unavailabilityCost.gt(web3.utils.toBN(0)), "Should have unavailability cost");
      assert.equal(metrics.invalidAddressAttempts + metrics.invalidRequirementAttempts, 2, "Should have caught invalid scenarios");
    });

    it("Should track data holding costs accurately", async () => {
      const holdingCost = web3.utils.toWei("0.0005", "ether");
      await analytics.updateDataHoldingCost(holdingCost, { from: owner });
      
      const dataHoldingCost = await analytics.dataHoldingCost();
      console.log("\n=== Storage Metrics ===");
      console.log("Data Holding Cost:", web3.utils.fromWei(dataHoldingCost), "ETH");
      
      assert.equal(dataHoldingCost.toString(), holdingCost, "Data holding cost should match");
    });
  });
});