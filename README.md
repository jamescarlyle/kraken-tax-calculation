# kraken-tax-calculation

The purpose of this codebase is to prepare data for capital gains tax and income tax in the UK, based on exported ledger data from kraken.com. The reason for 
this preparation is that kraken doesn't provide trades in GBP required for UK tax calculations.

It is assumed that the ledger data used as a basis for calculation is exported from kraken and saved as a CSV file in the /data directory. The filename can be 
specified on the command line when the calculations are run.

It is assumed that closing price data for assets are saved in the /data directory, using the filename pattern XXX-price-max.csv. The data for software development came from historical data at 
coingecko.com, e.g. https://www.coingecko.com/en/coins/xrp/historical_data?start=2015-07-31&end=2026-03-27. Note that the start and end dates can be specified in the URL requested.

The result of the CGT calculation is a CSV file which contains the matched data used by an existing CGT calculator, for example https://www.cgtcalculator.com/calculator.aspx. That is, data 
with columns for B/S, date, asset (ticker), quantity of asset, price (in GBP), and fees (in GBP).

The code is developed to run in [Marimo](https://marimo.io/), a python notebook environment. It's possible to use Marimo to export this as a Jupyter notebook, or standalone Python.
