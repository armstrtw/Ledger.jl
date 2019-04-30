##module Ledger

##using DataFrames, CategoricalArrays, CodecZlib, FileIO, TimeZones
using Dates, DataFrames, StatsBase

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

struct Position
units::Float64
value::Float64
derivatives_offset::Float64
end

struct PositionSE
unrealized_pnl::Float64
realized_pnl::Float64
end



mutable struct Ledger
positions::Dict{String,Position}
shareholder_equity::Dict{String,PositionSE}
Ledger() = new(Dict{String,Position}(),Dict{String,PositionSE}())
end

function initDerivative(positions::Dict{String,Position},ticker::String,quantity::Float64,value::Float64)
    @assert !haskey(positions,ticker)
    positions[ticker] = Position(quantity,value,-value)
end

function initPosition(positions::Dict{String,Position},ticker::String,quantity::Float64,value::Float64)
    @assert !haskey(positions,ticker)
    positions[ticker] = Position(quantity,value,0)
end

function debitCash(positions::Dict{String,Position},ccy::String,quantity::Float64)
    if !haskey(positions,ccy)
        l.positions[ccy] = Position(-quantity,-quantity,0)
    else
        l.positions[ccy] += Position(-quantity,-quantity,0)
    end
end

function updateLedger(l::Ledger,t::Transaction,secmaster::Dict{String,Security})
    s = secmaster[t.ticker]
    value = t.quantity * s.valuation(t.price)

    # init position has no SE impact
    if !haskey(l.positions,t.ticker)
        if s.is_derivative
            initDerivative(l.positions,t.ticker,t.quantity,value)
        else
            ## init position / debit cash
            initPosition(l.positions,t.ticker,t.quantity,value)
            debitCash(l.positions,s.settle_ccy,value)
        end
    end

    ## just for now
    @assert sum([x.value for x  in values(l.positions)])==0
    ##@assert countmap(l.positions)
    ##@assert values(l.positions)
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
