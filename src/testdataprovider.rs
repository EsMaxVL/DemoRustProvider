//! Implements `DataProvider` for `TestDataProvider`

use crate::DataProvider;

#[derive(Clone)]
pub struct TestDataProvider {}

#[async_trait::async_trait]
impl DataProvider for TestDataProvider {
    fn call_evm_view(
        &self,
        _chain: bs_common::EvmChain,
        _address: web3::types::Address,
        _calldata: &[u8],
    ) -> Result<bs_common::DataProviderResponse<Vec<u8>>, crate::error::DataProviderError>
    {
        todo!()
    }

    fn evm_block_number(
        &self,
        _chain: bs_common::EvmChain,
    ) -> Result<u64, crate::error::DataProviderError> {
        Ok(0)
    }

    fn evm_account_balance(
        &self,
        _chain: bs_common::EvmChain,
        _address: web3::types::Address,
    ) -> Result<
        bs_common::DataProviderResponse<web3::types::U256>,
        crate::error::DataProviderError,
    > {
        todo!()
    }
}
