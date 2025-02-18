const UncertaintyAnalytics = artifacts.require("UncertaintyAnalytics");
const RequestManager = artifacts.require("RequestManager");
const ResponseManager = artifacts.require("ResponseManager");

contract("UncertaintyAnalytics", function(accounts) {
  let analytics;
  let requestManager;
  let responseManager;
  const [owner, requester, responder] = accounts;

  const measureTime = (fn) => {
    console.log(">>> Measuring time for function");
    const start = Date.now();
    return fn().then(result => {
      const end = Date.now();
      console.log(`>>> Time measurement: ${end - start} ms`);
      return { result, duration: end - start };
    });
  };

  const getGasCost = (tx) => {
    console.log(">>> Calculating Gas Cost");
    return web3.eth.getTransactionReceipt(tx.tx)
      .then(receipt => {
        console.log("Transaction Receipt:", receipt);
        const gasCost = web3.utils.toBN(receipt.gasUsed).mul(web3.utils.toBN(tx.receipt.effectiveGasPrice));
        console.log("Gas Cost:", gasCost.toString());
        return gasCost;
      });
  };

  const advanceTime = () => {
    console.log(`>>> Advancing time by 86401 seconds`);
    return new Promise((resolve, reject) => {
      web3.currentProvider.send({
        jsonrpc: "2.0",
        method: "evm_increaseTime",
        params: [86401],
        id: new Date().getTime(),
      }, (err1) => {
        if (err1) return reject(err1);
        web3.currentProvider.send({
          jsonrpc: "2.0",
          method: "evm_mine",
          params: [],
          id: new Date().getTime()
        }, (err2) => {
          if (err2) return reject(err2);
          console.log(">>> Time advanced successfully");
          resolve();
        });
      });
    });
  };

  beforeEach(function(done) {
    this.timeout(0);
    UncertaintyAnalytics.new()
      .then(analyticsContract => {
        analytics = analyticsContract;
        return Promise.all([
          RequestManager.new(analytics.address),
          ResponseManager.new(analytics.address)
        ]);
      })
      .then(([requestManagerContract, responseManagerContract]) => {
        requestManager = requestManagerContract;
        responseManager = responseManagerContract;
        done();
      })
      .catch(done);
  });

  it("Should analyze full request-response cycle with metrics", function(done) {
    this.timeout(0);

    const metrics = {
      confirmationTimes: [],
      executionTimes: [],
      gasCosts: [],
      failedTransactions: 0,
      invalidAddressAttempts: 0,
      invalidRequirementAttempts: 0
    };

    console.log("\n=== Starting 10 Request-Response Cycles ===");

    // Create a promise chain for request-response cycles
    const requestCycles = Array(10).fill().reduce((chain, _, i) => {
      return chain.then(() => 
        measureTime(() => 
          requestManager.submitRequest({
            from: requester,
            value: web3.utils.toWei("0.001", "ether")
          })
        ).then(({ duration: confirmTime, result: tx }) => {
          metrics.confirmationTimes.push(confirmTime);
          return getGasCost(tx).then(gasCost => {
            metrics.gasCosts.push(gasCost);
            return measureTime(() => 
              responseManager.submitResponse(i + 1, { from: responder })
            ).then(({ duration: execTime }) => {
              metrics.executionTimes.push(execTime);
              
              if (i % 3 === 0) {
                return Promise.all([
                  analytics.updateDisruptionLevel(i + 1, { from: owner }),
                  analytics.updateEscalationLevel(Math.floor(i / 2), { from: owner })
                ]);
              }
            });
          });
        })
      );
    }, Promise.resolve());

    // Chain invalid scenarios and final metrics evaluation
    requestCycles
      .then(() => {
        console.log("\n=== Testing Invalid Scenarios ===");
        
        return requestManager.submitRequest({ 
          from: "0x0000000000000000000000000000000000000000",
          value: web3.utils.toWei("0.001", "ether")
        }).catch(() => {
          metrics.invalidAddressAttempts++;
        });
      })
      .then(() => {
        return requestManager.submitRequest({
          from: requester,
          value: web3.utils.toWei("0.0001", "ether")
        }).catch(() => {
          metrics.invalidRequirementAttempts++;
        });
      })
      .then(() => advanceTime())
      .then(() => analytics.calculateUnavailabilityCost(1))
      .then(() => Promise.all([
        analytics.getMetrics(),
        analytics.unavailabilityCost(),
        analytics.disruptionLevel(),
        analytics.escalationLevel()
      ]))
      .then(([analyticsMetrics, unavailabilityCost, disruptionLevel, escalationLevel]) => {
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
        
        done();
      })
      .catch(done);
  });

  it("Should track data holding costs accurately", function(done) {
    this.timeout(0);

    const holdingCost = web3.utils.toWei("0.0005", "ether");
    
    analytics.updateDataHoldingCost(holdingCost, { from: owner })
      .then(() => analytics.dataHoldingCost())
      .then((dataHoldingCost) => {
        console.log("\n=== Storage Metrics ===");
        console.log("Data Holding Cost:", web3.utils.fromWei(dataHoldingCost), "ETH");
        
        assert.equal(dataHoldingCost.toString(), holdingCost, "Data holding cost should match");
        
        done();
      })
      .catch(done);
  });
});