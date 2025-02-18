
import logging
import pandas as pd

class CompanyModule:
    def __init__(self, workflow):
        """
        Initialize CompanyModule with workflow context
        
        :param workflow: BlockchainWorkflow instance
        """
        self.workflow = workflow

    async def register_company_data(self, company_data):
        """
        Register company data on the blockchain
        
        :param company_data: List of company data dictionaries
        """
        try:
            # Validate contract availability
            if 'CompanyRegister' not in self.workflow.contracts:
                raise ValueError("CompanyRegister contract not loaded")

            contract = self.workflow.contracts['CompanyRegister']
            
            # Process each company record
            for record in company_data:
                try:
                    # Prepare transaction
                    tx_hash = await contract.functions.registerCompany(
                        record['company_name'],
                        record['registration_date'],
                        record['sector'],
                        float(record['emissions_baseline'])
                    ).transact({
                        'from': self.workflow.w3.eth.accounts[0],
                        'gas': 2000000
                    })
                    
                    # Wait for transaction receipt
                    receipt = await self.workflow.w3.eth.wait_for_transaction_receipt(tx_hash)
                    
                    # Log transaction
                    self.workflow.log_to_file('company_register_logs.json', record, receipt)
                    
                    logging.info(f"Registered company {record['company_name']} in {record['sector']}")
                
                except Exception as record_error:
                    logging.error(f"Error registering company record: {record_error}")
                    # Continue processing other records
                    continue

        except Exception as e:
            logging.error(f"Error in company data registration: {str(e)}")
            raise