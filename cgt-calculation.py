import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")

with app.setup:
    import pandas as pd
    import numpy as np
    FIAT = {'USD', 'GBP'}
    ASSETS_OF_INTEREST = {'BTC', 'ETH', 'SOL', 'SUI', 'LINK', 'XRP', 'ETC', 'BCH', 'DAI', 'ETHW', 'SCRT', 'STRK', 'EIGEN', 'NIGHT'}
    ASSET_EQUIVALENTS = {'ETH2': 'ETH', 'ETH2.S': 'ETH', 'USDC': 'USD'}
    constants = pd.DataFrame([
        {'snapped_at': pd.Timestamp(0, tz='UTC', unit='us'), 'asset': 'USD', 'price': 1.0},
        {'snapped_at': pd.Timestamp(0, tz='UTC', unit='us'), 'asset': 'USDC', 'price': 1.0},
        {'snapped_at': pd.Timestamp(0, tz='UTC', unit='us'), 'asset': 'USDT', 'price': 1.0},
        {'snapped_at': pd.Timestamp(0, tz='UTC', unit='us'), 'asset': 'NA', 'price': 0.0},
    ])
    # Note: should USDC be treated as equivalent to USD, and so all USDC entries get converted to USD and then treated as fiat, or should USDC remain unconverted but marked as fiat, or should USDC be treated as its own currency and have an exchange rate? Currently, treated as equivalent to USD, so no separate price is needed


@app.cell
def _():
    # Load all datasets. 
    # For the ledger, the sequence of timestamp isn't important, since the refID will be used to match.
    ledger_df = (pd.read_csv('./data/kraken_stocks_etfs_ledgers_2015-08-10-2026-03-26.csv', usecols=['txid', 'refid', 'time', 'type', 'asset', 'amount', 'fee', 'balance'], parse_dates=['time'])
        .rename(columns={'time': 'timestamp', 'amount': 'quantity'})
        .assign(timestamp=lambda x: x['timestamp'].dt.tz_localize('UTC'))
        .set_index(keys='refid', drop=False))
    # Convert all ETH2 to ETH
    ledger_df[['asset']] = ledger_df[['asset']].replace(ASSET_EQUIVALENTS)

    eth_price_df = (pd.read_csv('./data/eth-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='ETH'))
    btc_price_df = (pd.read_csv('./data/btc-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='BTC'))
    sol_price_df = (pd.read_csv('./data/sol-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='SOL'))
    sui_price_df = (pd.read_csv('./data/sui-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='SUI'))
    link_price_df = (pd.read_csv('./data/link-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='LINK'))
    xrp_price_df = (pd.read_csv('./data/xrp-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='XRP'))
    etc_price_df = (pd.read_csv('./data/etc-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='ETC'))
    bch_price_df = (pd.read_csv('./data/bch-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='BCH'))
    dai_price_df = (pd.read_csv('./data/dai-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='DAI'))
    ethw_price_df = (pd.read_csv('./data/ethw-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='ETHW'))
    scrt_price_df = (pd.read_csv('./data/scrt-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='SCRT'))
    strk_price_df = (pd.read_csv('./data/strk-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='STRK'))
    eigen_price_df = (pd.read_csv('./data/eigen-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='EIGEN'))
    night_price_df = (pd.read_csv('./data/night-usd-max.csv', usecols=['snapped_at', 'price'], parse_dates=['snapped_at'])
        .assign(asset='NIGHT'))

    all_asset_price_df = pd.concat([eth_price_df, btc_price_df, sol_price_df, sui_price_df, link_price_df, xrp_price_df, etc_price_df, bch_price_df, dai_price_df, ethw_price_df, scrt_price_df, strk_price_df, eigen_price_df, night_price_df, constants]).rename(columns={'snapped_at': 'timestamp'}).sort_values('timestamp')

    gbp_usd_df = (pd.read_csv('./data/gbp-usd-max.csv', usecols=['Date', 'Price'], parse_dates=['Date'], date_format='%d/%m/%Y')
        .rename(columns={'Date': 'timestamp', 'Price': 'price'})
        .assign(timestamp=lambda x: x['timestamp'].dt.tz_localize('UTC'))
        .sort_values('timestamp'))
    return all_asset_price_df, gbp_usd_df, ledger_df


@app.cell
def _(ledger_df):
    ledger_df['B/S'] = ledger_df['quantity'].apply(lambda x: 'B' if x > 0 else 'S')

    # Find paired transaction. Order of ledger_df doesn't matter at this stage.
    grouped_tx = ledger_df.groupby(level=0)['txid']
    ledger_df['pair_txid'] = grouped_tx.shift(-1).fillna(grouped_tx.shift(1)).fillna(ledger_df['txid'])
    ledger_df.set_index(keys='txid', inplace=True, drop=False)
    ledger_df['pair_asset'] = ledger_df['pair_txid'].map(ledger_df['asset'])
    ledger_df['pair_fee_quantity'] = ledger_df['pair_txid'].map(ledger_df['fee'])
    return


@app.cell
def _(ledger_df):
    # Determine which fee to use.

    # Define base masks.
    is_standalone = ledger_df['txid'] == ledger_df['pair_txid']
    is_asset_fiat = ledger_df['asset'].isin(FIAT)
    is_pair_fiat = ledger_df['pair_asset'].isin(FIAT)
    is_fee_zero = ledger_df['fee'] == 0
    is_pair_fee_zero = ledger_df['pair_fee_quantity'] == 0

    # Define the mutually exclusive conditions.
    fee_conditions = [
        # RULE 1: Standalone transactions (Withdrawals/Deposits/Solo entries)
        # Action: Keep own fee so it's not lost.
        is_standalone,
    
        # RULE 2: The "Sponge" (Crypto side of a Fiat trade)
        # Action: If Crypto has no fee, pull the fee from the Fiat side.
        ~is_asset_fiat & is_pair_fiat & is_fee_zero,
    
        # RULE 3: The "Donor" (Fiat side of a Fiat trade)
        # Action: If the Crypto side successfully pulled the fee (Rule 2), 
        # this row must result in 0 to prevent double-counting.
        is_asset_fiat & ~is_pair_fiat & is_pair_fee_zero,
    
        # RULE 4: Crypto-to-Crypto or Both-Sided Fees
        # Action: Everyone keeps their own fee.
        ~is_standalone
    ]

    # Define the outputs.
    fee_asset_options = [
        ledger_df['asset'],       # Rule 1: Keep own (Standalone)
        ledger_df['pair_asset'],  # Rule 2: Pull Fiat Asset
        'NA',                     # Rule 3: Zero out (Already accounted for in Rule 2)
        ledger_df['asset']        # Rule 4: Keep own (Standard trade)
    ]

    fee_quantity_options = [
        ledger_df['fee'],                # Rule 1: Keep own (Standalone)
        ledger_df['pair_fee_quantity'],  # Rule 2: Pull Fiat Quantity
        0,                               # Rule 3: Zero out
        ledger_df['fee']                 # Rule 4: Keep own (Standard trade)
    ]

    # Apply.
    ledger_df['allocated_fee_asset'] = np.select(fee_conditions, fee_asset_options, default=ledger_df['asset'])
    ledger_df['allocated_fee_quantity'] = np.select(fee_conditions, fee_quantity_options, default=ledger_df['fee'])
    return


@app.cell
def _(all_asset_price_df, gbp_usd_df, ledger_df):
    ledger_df[['allocated_fee_asset']] = ledger_df[['allocated_fee_asset']].replace(ASSET_EQUIVALENTS)

    ledger_gbp_df = (
        ledger_df.sort_values('timestamp')
     .pipe(pd.merge_asof, 
              all_asset_price_df[['timestamp', 'asset', 'price']].rename(columns={'price': 'asset_price_usd'}), 
              on='timestamp', 
              left_by='asset', 
              right_by='asset', 
              direction='backward')
        .pipe(pd.merge_asof, 
              all_asset_price_df[['timestamp', 'asset', 'price']].rename(columns={'price': 'fee_asset_price_usd', 'asset': 'asset_p'}),
              on='timestamp', 
              left_by='allocated_fee_asset', 
              right_by='asset_p', 
              direction='backward')
        .drop(columns=['asset_p'])
        .pipe(pd.merge_asof, 
              gbp_usd_df[['timestamp', 'price']].rename(columns={'price': 'gbp_price_usd'}), 
              on='timestamp', 
              direction='backward')
        .rename(columns={'price': 'gbp_price_usd'})
    )
    ledger_gbp_df.set_index(keys='txid', inplace=True)
    ledger_gbp_df['asset_price_gbp'] = ledger_gbp_df['asset_price_usd'] / ledger_gbp_df['gbp_price_usd']
    ledger_gbp_df['fee_gbp'] = ledger_gbp_df['allocated_fee_quantity'] * ledger_gbp_df['fee_asset_price_usd'] / ledger_gbp_df['gbp_price_usd']

    ledger_gbp_df['fee_gbp'] = ledger_gbp_df['fee_gbp'].mask(
        ledger_gbp_df['allocated_fee_asset'] == 'GBP', ledger_gbp_df['allocated_fee_quantity']
    )

    ledger_gbp_df['asset_price_gbp'] = ledger_gbp_df['asset_price_gbp'].mask(
        ledger_gbp_df['asset'] == 'GBP', 1.0
    )
    return (ledger_gbp_df,)

@app.cell
def _(ledger_gbp_df):
    ledger_gbp_df
    return

@app.cell
def _(ledger_gbp_df):
    (ledger_gbp_df[ledger_gbp_df['asset'].isin(ASSETS_OF_INTEREST)]
     [['B/S', 'timestamp', 'asset', 'quantity', 'asset_price_gbp', 'fee_gbp']]
     .assign(
         timestamp=lambda x: x['timestamp'].dt.strftime('%d/%m/%Y'),
         quantity=lambda x: x['quantity'].abs()
     )
     .to_csv('ledger_export.txt', sep='\t', header=False, index=False))

    # Use this output with https://www.cgtcalculator.com/calculator.aspx
    return

@app.cell
def _(ledger_df):
    ledger_df['asset'].unique()
    return

if __name__ == "__main__":
    app.run()