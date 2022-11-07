//! Implement data provider errors

use bs_common::DataReadError;
use solana_client::client_error::ClientError;

/// Errors related to data provider
#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum DataProviderError {
    #[error("Unsupported EVM chain")]
    UnsupportedEvmChain,
    #[error("Unsupported Solana chain")]
    UnsupportedSolanaChain,
    #[error("Web3 client error: {0}")]
    Web3ClientError(#[from] web3::Error),
    #[error("Solana client error: {0}")]
    SolanaClientError(#[from] solana_client::client_error::ClientError),
}

/// Broadcast errors from EVM-compatible chains.
pub fn dispatch_web3_errors<T>(
    res: web3::Result<T>,
) -> Result<bs_common::DataProviderResponse<T>, DataProviderError> {
    match res {
        Ok(x) => Ok(Ok(x)),
        Err(web3::Error::Decoder(e)) => Ok(Err(DataReadError::DecoderError(e))),
        Err(web3::Error::Rpc(e)) => Ok(Err(DataReadError::RpcError(e.to_string()))),
        Err(web3::Error::InvalidResponse(e)) => Ok(Err(DataReadError::InvalidResponse(e))),
        Err(e) => Err(DataProviderError::Web3ClientError(e)),
    }
}

/// Broadcast errors from Solana chain.
pub fn dispatch_solana_errors<T>(
    res: std::result::Result<T, ClientError>,
) -> Result<bs_common::DataProviderResponse<T>, DataProviderError> {
    // TODO
    match res {
        Ok(x) => Ok(Ok(x)),
        Err(e) => Err(DataProviderError::SolanaClientError(e)),
    }
}
