##module Ledger

##using DataFrames, CategoricalArrays, CodecZlib, FileIO, TimeZones
using Dates, DataFrames

struct Security
is_derivative::Bool
trade_ccy::String
settle_ccy::String
maturity::Union{Missing, Date}
valuation::Function
end

struct Transaction
asofdate::Date
ticker::String
quantity::Float64
price::Float64
end

mutable struct Ledger
## assets
holdings::Dict{String,Float64}

## liabilities
derivatives_settlement::Dict{String,Float64}

## shareholder equity
unrealized_pnl::Dict{String,Float64}
realized_pnl::Dict{String,Float64}
Ledger() = new(Dict{String,Float64}(), Dict{String,Float64}(), Dict{String,Float64}(), Dict{String,Float64}())
end


function updatePosition(ticker::String,quantity::Float64,holdings::Dict{String,Float64})
    ## needed b/c julia has no map += init
    if haskey(holdings,ticker)
        ## existing index, increment
        pos = holdings[ticker] += quantity

        ## if position is completely closed out, remove the index
        if pos == 0
            delete!(holdings,ticker)
        end
    else
        ## new entry, init
        l.holdings[ticker] = quantity
    end
end

## assets = liabilities + equity
function updateLedger(l::Ledger,t::Transaction,secmaster::Dict{String,Security})

    ## treatment of holding quantity is identical between cash and derivatives
    updatePosition(t.ticker,t.quantity,l.holdings)

    s = secmaster[t.ticker]
    settlement_cash = -t.quantity * s.valuation(t.price)

    if s.is_derivative
        ## derivative contra is tracked by ticker
        if haskey(l.derivatives_settlement,s.settle_ccy)
            derivatives_settlement[s.ticker] -= settlement_cash
        else
            derivatives_settlement[s.ticker] = settlement_cash
        end
    else
        ## if not a derivitive, debit cash
        updatePosition(s.settle_ccy,settlement_cash,l.holdings)
    end
end

# function run_pnl(transactions::Vector{Transaction},secmaster::Dict{String,Security},prices::NDSparse)

#     ## add checks
#     ## 1) all secmasters defined for unique(transaction.ticker)
#     ## 2) all prices available for position/date in prices    
    
#     # show(transactions)
#     # println()
#     # show(secmaster)
#     # println()
#     # show(prices)
#     # println()
#     ##return pricesix[Date(2019,1,2),"AMZN"].price

#     trade_dates = [x.asofdate for x in transactions];
#     start_date = minimum(trade_dates)
#     end_date = maximum(trade_dates)

#     ## run pnl for all weekdays in range
#     pnl_dates = filter(d -> dayofweek(d) != Dates.Saturday && dayofweek(d) != Dates.Sunday, start_date:Dates.Day(1):end_date)

#     holdings = Dict{String,Float64}()
#     derivatives_settlement: = Dict{String,Float64}()

#     transaction_idx = 1
#     ## loop dates, permute ledger for each date
#     ## run pnl for that day when next trade date is encountered
#     for pnl_date in pnl_dates
#         println("working on:",pnl_date)
#         while (transactions[transaction_idx].asofdate == pnl_date)
#             ticker = transactions[transaction_idx].ticker
#             updateLedgers(ticker,secmaster[ticker],holdings,derivatives_settlement)
#             transaction_idx += 1
#         end
#     end
# end



secmaster = Dict{String,Security}("AMZN" => Security(false,"USD","USD",missing,p -> p),
                                  "IBM" => Security(false,"USD","USD",missing,p -> p)
                                  )


## sec prices
ibm=DataFrame(ticker="IBM",asofdate=[Date(2019,1,2),Date(2019,1,3)],price=[100,101])
amzn=DataFrame(ticker="AMZN",asofdate=[Date(2019,1,2),Date(2019,1,3)],price=[200,201])
prices = [ibm;amzn]

trades = [Transaction(Date(2019,1,2,),"AMZN",1000,200),
          Transaction(Date(2019,1,20,),"IBM",10,100),
          Transaction(Date(2019,1,20,),"IBM",-10,100.50)
          ]



l = Ledger()

updateLedger(l,trades[1],secmaster)
##updateLedger(l,trades[2],secmaster)
##updateLedger(l,trades[3],secmaster)
print(l)



##end # module Ledger
